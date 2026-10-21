# ADR-003: AWS Deployment Shape — ECS Fargate for order-api

## Status

**Accepted** (2026-10-21; proposed 2026-07-18).
The deployment was built on ECS Fargate as proposed, and the App Runner fallback was not needed.

## Context

The public deployment covers the **synchronous slice** of the system:
order-api plus its PostgreSQL database, exposed at a public endpoint, provisioned entirely with Terraform, and disposable via `terraform destroy` between demo periods.

The **asynchronous slice stays local by design**:
inventory-worker, the outbox pollers, and the broker run in local Compose only, and the production story for the broker is argued in ADR-001 (managed Kafka assumption) rather than deployed.
Deploying a broker plus consumers would multiply infrastructure cost and Terraform surface without changing what the deployment demonstrates — the event-driven machinery is exercised locally and in CI (§2), where it is reproducible for free.

A public write endpoint with no authentication is a real cost and abuse surface even for a demo.
The proposal left it open, to be settled when the deployment was built; the Decision below settles it.

Deployment shape candidates for order-api, from most to least self-managed:

1. **EC2** — self-managed instances; container orchestration by hand.
2. **EKS** — managed Kubernetes.
3. **ECS on Fargate** — AWS-native orchestration with a serverless data plane; networking (VPC, ALB, security groups) and IAM remain first-party Terraform.
4. **App Runner** — container PaaS; HTTPS endpoint, load balancing, TLS, and autoscaling are built in, networking is hidden.

The database is **RDS PostgreSQL** in a private subnet in every variant; it is not part of this decision.

## Decision

- **Primary: ECS Fargate** — one Fargate service for order-api behind an ALB, RDS PostgreSQL in a private subnet, all provisioned by Terraform (VPC, subnets, security groups, the task execution role, ECR, CloudWatch Logs).
- **Documented fallback: App Runner** (not used) — if the implementation exceeds its time box, the deployment falls back to an App Runner service (same container image, same RDS, VPC connector for database egress).
  The fallback is a scope reduction, not a redesign: the image and the Terraform-managed RDS carry over.
- **The broker is not deployed** — Kafka remains local Compose; production assumption documented in ADR-001.

What the implementation settled:

- **Endpoint protection: an address allow-list and the destroy lifecycle, not authentication.**
  The load balancer accepts port 80 only from the CIDR blocks in `allowed_cidrs`, a variable with no default, so every `plan` and `apply` names who may connect.
  Outside a demo period the stack does not exist.
  The endpoint is therefore never open to the internet at large, which is the exposure domain-design §7 makes authentication a prerequisite for; authentication itself stays deferred with the rest of the authenticated principal.
- **Plain HTTP.**
  A certificate needs a domain, which this deployment does not have.
  A production deployment adds port 443 with an ACM certificate and redirects port 80 to it.
- **One IAM role, the execution role.**
  order-api calls no AWS API, so there is no task role.
  The execution role's inline policy names the one image repository, the one log group and the database secret; only `ecr:GetAuthorizationToken` takes `*`, because that action has no resource to scope to.
- **Redis as a second container in the task**, reached on `localhost`.
  It holds the HTTP idempotency response cache only; the durable authority is the database constraint (domain-design §4.4), so losing the cache with the task loses nothing that matters.
- **Migrations and the seed as one-shot tasks**, started with `aws ecs run-task` from task definitions of their own.
  The database is in a private subnet and cannot be reached from outside the VPC.
- **No automated deployment.**
  A deploy job would fail every time the stack has been destroyed; the steps in the README are run by hand.

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

- Terraform is the deliverable as much as the running service: VPC, ALB, security groups, the IAM role, ECS, RDS, ECR, and logs are all declared in-repo.
  `terraform destroy` removes all 35 resources.
  The tag-based resource search keeps listing deleted resources for a while afterwards, so that nothing is left is checked against each service directly.
- The deployed system is the synchronous slice only; a reader probing the async design is pointed at local Compose, CI, and ADR-001/ADR-002 — this split (deploy the API, argue the broker) is now explicit and deliberate.
  A consequence to make visible when demoing: with no broker or worker in the cloud, a confirmed order stays `CONFIRMED` (its reservation never runs) and its outbox rows accumulate unsent — the full Saga is shown only against local Compose / CI.
- Fargate's idle cost (ALB floor) is accepted and bounded by the destroy lifecycle.
  A 29-minute deployment on 2026-10-20 cost about USD 0.04 in Cost Explorer, more than half of it the load balancer.
- App Runner stays recorded as the scope reduction available if the ECS surface ever has to shrink; the image and the RDS would carry over unchanged.

## References

- `docs/design/domain-design.md` §2, §4.4, §5.4, §7 (public deployment deferral, authentication)
- ADR-001 (production broker assumption), ADR-002 (poller as a resident process)
- [ECS on Fargate](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html)
- [App Runner](https://docs.aws.amazon.com/apprunner/latest/dg/what-is-apprunner.html) / [VPC connector](https://docs.aws.amazon.com/apprunner/latest/dg/network-vpc.html)
