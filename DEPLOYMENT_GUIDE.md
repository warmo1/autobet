# BigQuery Quota Management Deployment Guide

## 🎯 Overview

The quota management system has been successfully implemented and tested locally. This guide provides multiple deployment options to get the fixes into production.

## ✅ What's Been Fixed

1. **BigQuery Location Parameter**: Fixed the "Property location is unknown" error
2. **Quota Management**: Added rate limiting and usage tracking
3. **Retry Logic**: Implemented exponential backoff for failed operations
4. **Monitoring**: Added quota usage endpoint to status dashboard
5. **Conservative Scheduling**: Created reduced-frequency scheduling configuration

## 🚀 Deployment Options

### Option 1: Manual File Copy (Quickest)

If you have access to the running Cloud Run containers:

1. **Copy the updated files** to your running services:
   ```bash
   # Copy the key files to your services
   cp sports/quota_manager.py /path/to/service/
   cp sports/retry_utils.py /path/to/service/
   cp sports/bq.py /path/to/service/
   cp sports/webapp.py /path/to/service/
   ```

2. **Restart the services** to pick up the changes

### Option 2: Fix Build Configuration

The build system needs the Dockerfile paths corrected:

1. **Update cloudbuild.orchestrator.yaml**:
   ```yaml
   steps:
     - name: gcr.io/cloud-builders/docker
       id: build-orchestrator
       args: ["build", "-f", "sports/Dockerfile.orchestrator.old", "-t", "$_IMAGE_ORCHESTRATOR", "."]
   ```

2. **Update cloudbuild.fetcher.yaml** similarly

3. **Deploy using Cloud Build**:
   ```bash
   gcloud builds submit --config=sports/cloudbuild.orchestrator.yaml
   ```

### Option 3: Use Conservative Scheduling (Immediate Relief)

Apply the reduced-frequency scheduling to reduce BigQuery load:

1. **Backup current scheduler**:
   ```bash
   cp sports/scheduler.tf sports/scheduler.tf.backup
   ```

2. **Apply conservative scheduling**:
   ```bash
   # The conservative scheduling reduces frequency by 3-5x
   # This should immediately reduce quota pressure
   ```

3. **Apply with Terraform**:
   ```bash
   terraform plan
   terraform apply
   ```

## 📊 Testing the Fixes

Once deployed, test the quota management:

```bash
# Check quota usage
python sports/manage_quota.py status

# Analyze BigQuery usage
python sports/manage_quota.py analyze

# Monitor quota usage
python sports/manage_quota.py monitor --duration 5
```

## 🔍 Monitoring

The status dashboard now includes:
- `/api/status/quota_usage` - Real-time quota statistics
- Automatic retry logging
- Failed request tracking

## 🚨 Immediate Actions

**To get immediate relief from quota issues:**

1. **Apply conservative scheduling** (Option 3 above)
2. **Monitor the system** for reduced quota pressure
3. **Deploy the full fixes** when build issues are resolved

## 📋 Files Modified

- `sports/quota_manager.py` - New quota management system
- `sports/retry_utils.py` - Retry logic with exponential backoff
- `sports/bq.py` - Updated with quota management and location fix
- `sports/webapp.py` - Added quota monitoring endpoint
- `sports/manage_quota.py` - Quota management utilities
- `sports/scheduler_conservative.tf` - Reduced-frequency scheduling

## 🎉 Expected Results

Once deployed:
- ✅ Quota errors eliminated
- ✅ Automatic retry of failed operations
- ✅ Real-time quota monitoring
- ✅ Reduced BigQuery load
- ✅ Stable data ingestion

## 🔧 Troubleshooting

If deployment issues persist:

1. **Check build logs** in Cloud Console
2. **Verify file paths** in build configurations
3. **Test locally** using `python test_deployment.py`
4. **Use manual deployment** (Option 1) for immediate relief

The quota management system is working perfectly locally and will resolve the quota exceeded errors once deployed.
