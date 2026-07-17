# ADR-003: AWS Deployment Shape — ECS Fargate for order-api

## Status

**Proposed** (draft — 2026-07-18).
To be finalized when the deployment is implemented (planned October 2026, see domain-design §7);
the fallback trigger below is part of the proposal.

## Context

The public deployment covers the **synchronous slice** of the system:
order-api plus its PostgreSQL database, exposed at a public endpoint, provisioned entirely with Terraform, and disposable via `terraform destroy` between demo periods.

The **asynchronous slice stays local by design**:
inventory-worker, the outbox pollers, and the broker run in local Compose only, and the production story for the broker is argued in ADR-001 (managed Kafka assumption) rather than deployed.
Deploying a broker plus consumers would multiply infrastructure cost and Terraform surface without changing what the deployment demonstrates — the event-driven machinery is exercised locally and in CI (§2), where it is reproducible for free.

Deployment shape candidates for order-api, from most to least self-managed:

1. **EC2** — self-managed instances; container orchestration by hand.
2. **EKS** — managed Kubernetes.
3. **ECS on Fargate** — AWS-native orchestration with a serverless data plane; networking (VPC, ALB, security groups) and IAM remain first-party Terraform.
4. **App Runner** — container PaaS; HTTPS endpoint, load balancing, TLS, and autoscaling are built in, networking is hidden.

The database is **RDS PostgreSQL** in a private subnet in every variant; it is not part of this decision.

## Decision (proposed)

- **Primary: ECS Fargate** — one Fargate service for order-api behind an ALB, RDS PostgreSQL in a private subnet, all provisioned by Terraform (VPC, subnets, security groups, IAM task/execution roles, ECR, CloudWatch Logs).
- **Documented fallback: App Runner** — if the implementation exceeds its time box, the deployment falls back to an App Runner service (same container image, same RDS, VPC connector for database egress).
  The fallback is a scope reduction, not a redesign: the image and the Terraform-managed RDS carry over.
- **The broker is not deployed** — Kafka remains local Compose; production assumption documented in ADR-001.

## Rationale

### Why not EC2 or EKS

- **EC2** re-introduces instance management (patching, capacity, container runtime) that every option above it absorbs; nothing in a single-service deployment justifies it.
- **EKS** adds a Kubernetes control plane — a fixed hourly cost and an operational vocabulary (nodes, deployments, ingress controllers) — to run **one** stateless service.
  Kubernetes knowledge is valuable but is not what this deployment exists to demonstrate; ECS covers the orchestration need with far less surface.

The realistic choice is between the two managed-container shapes: **ECS Fargate vs App Runner**.

### ECS Fargate vs App Runner

| Factor | ECS Fargate | App Runner |
|---|---|---|
| Networking (VPC, LB, SG) | First-party Terraform | Hidden inside the service |
| IAM granularity | Task role vs execution role, explicit | Simplified service roles |
| Workload shapes | Any long-running container | HTTP request–response only |
| Terraform surface | Large (tens of resources) | Small (service + VPC connector) |
| Idle cost while deployed | Task cost + fixed ALB floor | Provisioned-instance fee only |
| Time to first deploy | Days | Hours |

Three considerations decide the primary path:

- **Workload-shape asymmetry.**
  App Runner serves HTTP request–response workloads only; outside request handling the container's CPU is throttled, so resident loops — a Kafka consumer, an outbox poller (§5.4) — cannot run there.
  The current scope (order-api only) fits either platform, but any future step that moves the asynchronous slice to AWS (workers and pollers alongside managed Kafka, ADR-001) is only reachable from ECS.
  Choosing Fargate keeps the deployment on the same platform that scenario would use; choosing App Runner would make it a dead end to migrate away from.
- **Demonstration surface.**
  The point of deploying at all is to exercise the production-shaped building blocks — VPC layout, security-group chains, IAM role separation, load balancing — as first-party Terraform code in this repository.
  App Runner's convenience consists precisely of hiding those blocks; what it saves is what this deployment exists to show.
- **Cost is real but not decisive.**
  The always-on ALB gives Fargate a fixed idle-cost floor that App Runner avoids;
  the lifecycle policy (destroy between demo periods) caps that difference to demo windows, so it does not outweigh the two factors above.

### The fallback contract

The App Runner fallback exists because the Fargate path's Terraform surface is the schedule risk, and a working public endpoint matters more than the full networking demonstration.
Falling back trades away the VPC/ALB/IAM surface and the future worker path — and keeps everything else:
the same container image, the same Terraform-managed RDS, and the same public CRUD behaviour.
The trigger is schedule-based (time box exceeded), not technical; no property of the system rules App Runner out for the API-only scope.

## Consequences

- Terraform is the deliverable as much as the running service: VPC, ALB, security groups, IAM roles, ECS, RDS, ECR, and logs are all declared in-repo; `terraform destroy` must leave nothing behind (verified as part of the deployment task).
- The deployed system is the synchronous slice only; a reader probing the async design is pointed at local Compose, CI, and ADR-001/ADR-002 — this split (deploy the API, argue the broker) is now explicit and deliberate.
- Fargate's idle cost (ALB floor) is accepted and bounded by the destroy lifecycle.
- If the fallback fires, this ADR is amended: status stays Accepted with the App Runner outcome recorded and the Fargate path preserved as the documented growth direction.

## References

- `docs/design/domain-design.md` §2, §5.4, §7 (public deployment deferral)
- ADR-001 (production broker assumption), ADR-002 (poller as a resident process)
- [ECS on Fargate](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html)
- [App Runner](https://docs.aws.amazon.com/apprunner/latest/dg/what-is-apprunner.html) / [VPC connector](https://docs.aws.amazon.com/apprunner/latest/dg/network-vpc.html)
