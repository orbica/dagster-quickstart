# Deploy
```
gcloud beta run jobs deploy job-quickstart \
  --source . \
  --tasks 50 \
  --set-env-vars SLEEP_MS=10000 \
  --set-env-vars FAIL_RATE=0.1 \
  --set-env-vars DATA_PATH=/workspace/data \
  --max-retries 5 \
  --add-volume name=data,type=cloud-storage,bucket=dagster-test-astute-fort-412223 \
  --add-volume-mount volume=data,mount-path=/workspace/data \
  --region us-central1 \
  --project=astute-fort-412223
```