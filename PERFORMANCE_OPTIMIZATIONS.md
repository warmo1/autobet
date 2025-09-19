# Autobet Performance Optimizations

This document describes the performance optimizations implemented to improve race status update timing and overall system responsiveness.

## 🚀 Overview

The optimizations focus on:
- **Faster race status updates** (2-5 minutes during peak hours)
- **Real-time status monitoring** and anomaly detection
- **Peak hours-aware caching** (2 minutes during peak, 5 minutes off-peak)
- **Enhanced materialized view refresh** (60 minutes instead of 720 minutes)
- **Comprehensive monitoring** and performance tracking

## 📊 Key Improvements

### Before Optimizations
- Materialized views refreshed every 12 hours
- Cache refresh every 5 minutes (fixed)
- No status anomaly detection
- Limited visibility into update timing

### After Optimizations
- Materialized views refreshed every 60 minutes
- Cache refresh every 2 minutes during peak hours (7 AM - 10 PM UK time)
- Real-time status monitoring every 2 minutes during peak hours
- Comprehensive anomaly detection and reporting
- Peak hours-aware refresh intervals

## 🏗️ Architecture

### New Components

1. **Materialized Views**
   - `mv_latest_win_odds_fast` - Faster odds refresh (60 minutes)
   - `mv_race_status_monitor` - Real-time status monitoring (5 minutes)

2. **Monitoring Views**
   - `vw_races_should_be_closed` - Identifies races that should be closed
   - `vw_races_should_be_open` - Identifies races that should be open
   - `vw_status_update_performance` - Performance metrics
   - `vw_peak_racing_hours` - Peak hours detection

3. **Cloud Scheduler Jobs**
   - `race-status-monitor` - Every 2 minutes during peak hours
   - `fast-cache-refresh` - Every 2 minutes during peak hours
   - `superfecta-live` - Every 5 minutes (increased from 10)

4. **Monitoring Scripts**
   - `monitor_race_status.py` - Real-time status monitoring
   - `check_performance_status.py` - Performance status checker
   - Enhanced `refresh_cache.py` - Peak hours-aware caching

## 🚀 Deployment

### Quick Deploy
```bash
# Deploy all optimizations
./scripts/deploy_performance_optimizations.sh
```

### Manual Deploy Steps

1. **Deploy BigQuery Optimizations**
   ```bash
   bq query --use_legacy_sql=false --project_id=autobet-470818 < performance_optimization.sql
   ```

2. **Deploy Terraform Changes**
   ```bash
   cd sports
   terraform plan -out=tfplan
   terraform apply tfplan
   ```

3. **Deploy Cloud Run Services**
   ```bash
   cd sports
   gcloud builds submit --config cloudbuild.orchestrator.yaml --project=autobet-470818
   ```

## 📈 Monitoring

### Check Performance Status
```bash
# View current performance metrics
python scripts/check_performance_status.py
```

### Monitor Race Status
```bash
# Run real-time status monitoring
python scripts/monitor_race_status.py
```

### Check Cache Performance
```bash
# Run cache refresh with peak hours logic
python scripts/refresh_cache.py
```

## 🔧 Configuration

### Environment Variables
- `CACHE_REFRESH_INTERVAL` - Base cache refresh interval (default: 300 seconds)
- `CACHE_CLEANUP_INTERVAL` - Cache cleanup interval (default: 3600 seconds)

### Peak Hours Configuration
Peak hours are automatically detected based on UK time (7 AM - 10 PM):
- **Peak Hours**: 2-minute refresh intervals
- **Off-Peak Hours**: 5-minute refresh intervals

## 📊 Performance Metrics

### Expected Improvements
- **Race Status Updates**: 2-5 minutes during peak hours
- **Cache Refresh**: 2 minutes during peak hours
- **Materialized Views**: 60 minutes refresh (vs 720 minutes)
- **Status Accuracy**: Real-time monitoring and reporting

### Monitoring Views
- `vw_status_update_performance` - Overall accuracy metrics
- `vw_races_should_be_closed` - Races that need attention
- `vw_races_should_be_open` - Races that may be prematurely closed
- `vw_peak_racing_hours` - Current peak hours status

## 🚨 Troubleshooting

### Common Issues

1. **Status Anomalies**
   - Check `vw_races_should_be_closed` for races that should be closed
   - Check `vw_races_should_be_open` for races that should be open
   - Review logs for status monitoring errors

2. **Performance Issues**
   - Check `vw_status_update_performance` for accuracy metrics
   - Verify materialized view refresh times
   - Check Cloud Scheduler job execution

3. **Cache Issues**
   - Verify peak hours detection is working
   - Check cache refresh logs
   - Monitor refresh interval adjustments

### Debug Commands
```bash
# Check BigQuery view status
bq query --use_legacy_sql=false "SELECT * FROM \`autobet-470818.autobet.vw_status_update_performance\`"

# Check peak hours status
bq query --use_legacy_sql=false "SELECT * FROM \`autobet-470818.autobet.vw_peak_racing_hours\`"

# Check materialized view status
bq query --use_legacy_sql=false "SELECT table_name, last_modified_time FROM \`autobet-470818.autobet.INFORMATION_SCHEMA.TABLES\` WHERE table_name LIKE 'mv_%'"
```

## 📝 Maintenance

### Regular Tasks
1. **Monitor Performance**: Run `check_performance_status.py` daily
2. **Review Anomalies**: Check status monitoring views for issues
3. **Adjust Intervals**: Modify refresh intervals based on performance
4. **Update Views**: Refresh materialized views as needed

### Scaling Considerations
- Monitor BigQuery costs with increased refresh frequency
- Adjust peak hours detection based on actual racing patterns
- Consider additional monitoring for high-traffic periods

## 🔄 Rollback

If issues occur, you can rollback by:
1. Reverting Terraform changes
2. Restoring original materialized view refresh intervals
3. Disabling new Cloud Scheduler jobs
4. Reverting to original cache refresh logic

## 📞 Support

For issues or questions:
1. Check the monitoring views for current status
2. Review Cloud Scheduler job logs
3. Check BigQuery query logs for performance issues
4. Review the performance optimization SQL for configuration details

---

**Last Updated**: $(date)
**Version**: 1.0
**Status**: Production Ready
