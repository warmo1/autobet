# Monitoring and Alerting for WebSocket Architecture

# Log-based metrics for WebSocket service
resource "google_logging_metric" "websocket_connection_errors" {
  name   = "websocket_connection_errors"
  filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND severity>=ERROR"
  
  metric_descriptor {
    metric_kind = "COUNTER"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "websocket_messages_processed" {
  name   = "websocket_messages_processed"
  filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND jsonPayload.message=~\"Processed.*event\""
  
  metric_descriptor {
    metric_kind = "COUNTER"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "pubsub_messages_published" {
  name   = "pubsub_messages_published"
  filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND jsonPayload.message=~\"Published.*to.*\""
  
  metric_descriptor {
    metric_kind = "COUNTER"
    value_type  = "INT64"
  }
}

# Alerting policies
resource "google_monitoring_alert_policy" "websocket_service_down" {
  display_name = "WebSocket Service Down"
  combiner     = "OR"
  
  conditions {
    display_name = "WebSocket service not responding"
    condition_threshold {
      filter          = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription"
      duration        = "300s"
      comparison      = "COMPARISON_LT"
      threshold_value = 1
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
  
  notification_channels = []
  
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "websocket_high_error_rate" {
  display_name = "WebSocket High Error Rate"
  combiner     = "OR"
  
  conditions {
    display_name = "High error rate in WebSocket service"
    condition_threshold {
      filter          = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND severity>=ERROR"
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 10
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
  
  notification_channels = []
  
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "pubsub_message_backlog" {
  display_name = "Pub/Sub Message Backlog"
  combiner     = "OR"
  
  conditions {
    display_name = "High Pub/Sub message backlog"
    condition_threshold {
      filter          = "resource.type=pubsub_subscription"
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 1000
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = []
  
  alert_strategy {
    auto_close = "1800s"
  }
}

# Dashboard for WebSocket monitoring
resource "google_monitoring_dashboard" "websocket_dashboard" {
  dashboard_json = jsonencode({
    displayName = "WebSocket Architecture Monitoring"
    mosaicLayout = {
      tiles = [
        {
          width  = 6
          height = 4
          widget = {
            title = "WebSocket Service Health"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription"
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Requests/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          width  = 6
          height = 4
          xPos   = 6
          widget = {
            title = "WebSocket Messages Processed"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND jsonPayload.message=~\"Processed.*event\""
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Messages/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          width  = 6
          height = 4
          yPos   = 4
          widget = {
            title = "Pub/Sub Messages Published"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND jsonPayload.message=~\"Published.*to.*\""
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Messages/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          width  = 6
          height = 4
          xPos   = 6
          yPos   = 4
          widget = {
            title = "WebSocket Errors"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND severity>=ERROR"
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Errors/sec"
                scale = "LINEAR"
              }
            }
          }
        }
      ]
    }
  })
}
