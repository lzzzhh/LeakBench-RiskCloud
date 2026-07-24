# Minimal Cloud VM Deployment

## Prerequisites (user must complete)

1. Create an Ubuntu 24.04 LTS VM (4 vCPU, 16 GB RAM, 50 GB disk)
2. Configure SSH key and firewall (inbound port 22 only)
3. SSH into the VM

## Deployment Steps

```bash
# 1. Install Git and clone
sudo apt-get update
sudo apt-get install -y git
git clone https://github.com/lzzzhh/LeakBench-RiskCloud.git "${HOME}/LeakBench-RiskCloud"
cd "${HOME}/LeakBench-RiskCloud"

# 2. Bootstrap Docker
bash deploy/vm/bootstrap.sh

# IMPORTANT: Exit SSH and reconnect for Docker group to take effect
exit
```

After reconnecting:

```bash
cd "${HOME}/LeakBench-RiskCloud"

# 3. Deploy (checkout + build)
bash deploy/vm/deploy.sh

# 4. Run the demo
bash deploy/vm/run_demo.sh

# 5. Verify results
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
docker compose down
rm -rf data/warehouse data/artifacts
```

## Estimated Costs

Costs depend on cloud provider, region, instance type, and disk type.
Example budget for demonstration purposes — check your platform's real-time
pricing before creating resources:

- VM (4 vCPU, 16 GB): ~$0.25-0.50/hour
- Disk (50 GB): ~$5-10/month
- 2-hour demo: ~$1-2 USD

**Stop or delete the VM after testing to avoid ongoing charges.**
