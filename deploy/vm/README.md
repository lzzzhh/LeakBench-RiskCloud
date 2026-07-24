# Minimal Cloud VM Deployment

## Prerequisites (user must complete)

1. Create an Ubuntu 24.04 LTS VM (4 vCPU, 16 GB RAM, 50 GB disk)
2. Configure SSH key and firewall (inbound port 22 only)
3. SSH into the VM

## Deployment Steps

```bash
# 1. Bootstrap the VM
bash deploy/vm/bootstrap.sh

# 2. Deploy (clone + build)
bash deploy/vm/deploy.sh

# 3. Run the demo
bash deploy/vm/run_demo.sh

# 4. Verify results
bash deploy/vm/verify.sh
```

## Expected Output

```
Prediction Points: 30
Feature Values: 600
Feature IDs: 20
WOE Rules: > 0
```

## Cleanup

```bash
# Stop Docker containers
docker compose down

# Remove persisted data (optional)
rm -rf data/warehouse data/artifacts
```

## Estimated Costs

- VM: ~$0.25-0.50/hour (4 vCPU, 16 GB)
- Storage: ~$5-10/month (50 GB)
- Total for 2-hour demo: ~$1-2 USD

Remember to stop or delete the VM after testing to avoid ongoing charges.
