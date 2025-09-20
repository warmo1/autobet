# WebSocket Architecture for Real-time Data

This document describes the WebSocket architecture implemented for real-time data ingestion from the Tote API.

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Tote API      │    │  WebSocket       │    │   Cloud Run     │
│   WebSocket     │◄───┤  Subscription    │    │   Service       │
│                 │    │  Service         │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   Pub/Sub        │    │   BigQuery      │
                       │   Topics         │    │   (Data Store)  │
                       └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Web App        │
                       │   (SSE Updates)  │
                       └──────────────────┘
```

## Components

### 1. WebSocket Subscription Service (`websocket-subscription`)

**Location**: `sports/websocket_service.py`

**Purpose**: Dedicated Cloud Run service that maintains persistent WebSocket connections to the Tote API.

**Features**:
- Handles all WebSocket message types from Tote API
- Publishes events to Pub/Sub topics
- Automatic reconnection with exponential backoff
- Health monitoring and status endpoints
- BigQuery integration for data persistence

**Endpoints**:
- `GET /health` - Health check
- `POST /start` - Start WebSocket subscription
- `POST /stop` - Stop WebSocket subscription
- `GET /status` - Get subscription status
- `POST /test-pubsub` - Test Pub/Sub publishing

### 2. Pub/Sub Topics

**Real-time Event Topics**:
- `tote-pool-total-changed` - Pool total updates
- `tote-product-status-changed` - Product status changes
- `tote-event-status-changed` - Event status changes
- `tote-event-result-changed` - Event results
- `tote-pool-dividend-changed` - Dividend updates
- `tote-selection-status-changed` - Selection status changes
- `tote-competitor-status-changed` - Competitor status changes
- `tote-bet-lifecycle` - Bet lifecycle events

### 3. Web App Integration

**Location**: `sports/pubsub_consumer.py`

**Purpose**: Consumes Pub/Sub messages and publishes to in-memory event bus for SSE.

**Features**:
- Automatic subscription to all event topics
- Error handling and retry logic
- Integration with existing SSE infrastructure

## Message Flow

1. **WebSocket Connection**: Service connects to Tote API WebSocket
2. **Message Processing**: Incoming messages are parsed and validated
3. **Data Persistence**: Messages are stored in BigQuery for historical analysis
4. **Event Publishing**: Processed events are published to Pub/Sub topics
5. **Web App Consumption**: Web app consumes from Pub/Sub and serves via SSE
6. **Real-time Updates**: UI receives real-time updates through SSE

## Supported Message Types

Based on the [Tote API documentation](https://developers.services.tote.co.uk/subscriptions/messages/):

### Bet Messages
- `BetAccepted` - Bet was accepted
- `BetRejected` - Bet was rejected
- `BetFailed` - Bet failed to place
- `BetCancelled` - Bet was cancelled
- `BetResulted` - Bet was resulted
- `BetSettled` - Bet was settled

### Product Messages
- `ProductStatusChanged` - Product status changed (open/closed/settled)
- `SelectionStatusChanged` - Individual selection status changed
- `PoolTotalChanged` - Pool total amount changed
- `PoolDividendChanged` - Pool dividends changed

### Event Messages
- `EventStatusChanged` - Event status changed
- `EventResultChanged` - Event results changed
- `CompetitorStatusChanged` - Competitor status changed

## Deployment

### Prerequisites
- Google Cloud Project with required APIs enabled
- Terraform configured
- Docker images built and pushed

### Deploy All Components
```bash
./scripts/deploy_websocket_architecture.sh
```

### Manual Deployment Steps
1. **Build and Push Images**:
   ```bash
   make build-and-push-gcb
   ```

2. **Deploy Infrastructure**:
   ```bash
   cd sports
   terraform init
   terraform apply -var "project_id=YOUR_PROJECT" -var "region=YOUR_REGION"
   ```

3. **Update Services**:
   ```bash
   make set-images
   ```

4. **Start WebSocket Service**:
   ```bash
   curl -X POST https://websocket-subscription-xxx.run.app/start
   ```

## Monitoring

### Cloud Monitoring Dashboard
- WebSocket service health metrics
- Message processing rates
- Error rates and logs
- Pub/Sub message backlog

### Logs
```bash
# WebSocket service logs
gcloud logs tail --follow --project=YOUR_PROJECT \
  --filter='resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription'

# Pub/Sub message logs
gcloud logs tail --follow --project=YOUR_PROJECT \
  --filter='resource.type=pubsub_subscription'
```

### Health Checks
```bash
# Check service health
curl https://websocket-subscription-xxx.run.app/health

# Check subscription status
curl https://websocket-subscription-xxx.run.app/status

# Test Pub/Sub publishing
curl -X POST https://websocket-subscription-xxx.run.app/test-pubsub
```

## Configuration

### Environment Variables
- `BQ_PROJECT` - BigQuery project ID
- `BQ_DATASET` - BigQuery dataset name
- `TOTE_API_KEY` - Tote API key (from Secret Manager)
- `TOTE_GRAPHQL_URL` - Tote GraphQL endpoint (from Secret Manager)
- `TOTE_SUBSCRIPTIONS_URL` - Tote WebSocket endpoint (from Secret Manager)

### Scheduler Jobs
- `websocket-start` - Starts WebSocket service at 06:00 UK time
- `websocket-stop` - Stops WebSocket service at 23:00 UK time

## Troubleshooting

### Common Issues

1. **WebSocket Connection Failed**
   - Check API credentials in Secret Manager
   - Verify network connectivity
   - Check service logs for authentication errors

2. **No Messages Received**
   - Verify WebSocket service is running
   - Check Pub/Sub topic subscriptions
   - Verify message routing in web app

3. **High Memory Usage**
   - Check for message backlog in Pub/Sub
   - Verify BigQuery write performance
   - Monitor service resource limits

### Debug Commands
```bash
# Check service status
curl https://websocket-subscription-xxx.run.app/status

# View recent logs
gcloud logs read --project=YOUR_PROJECT --limit=50 \
  --filter='resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription'

# Check Pub/Sub topics
gcloud pubsub topics list --project=YOUR_PROJECT

# Check subscriptions
gcloud pubsub subscriptions list --project=YOUR_PROJECT
```

## Cost Optimization

### Resource Limits
- WebSocket service: 2 CPU, 1GB RAM
- Pub/Sub: Pay per message
- BigQuery: Pay per query and storage

### Scheduling
- Service runs only during peak hours (06:00-23:00 UK time)
- Automatic scaling to zero when not in use
- Efficient message batching for BigQuery writes

## Security

### Authentication
- Service account with minimal required permissions
- API keys stored in Secret Manager
- Pub/Sub message encryption in transit

### Network Security
- Cloud Run ingress controls
- VPC connector for private resources (if needed)
- TLS encryption for all communications

## Future Enhancements

1. **Message Filtering**: Add topic-based filtering for specific events
2. **Rate Limiting**: Implement rate limiting for high-volume events
3. **Message Deduplication**: Add deduplication for duplicate messages
4. **Multi-region**: Deploy across multiple regions for redundancy
5. **Custom Dashboards**: Create custom monitoring dashboards
6. **Alerting**: Add email/SMS alerts for critical events
