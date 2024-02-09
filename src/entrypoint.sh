#!/bin/bash

# Check environment variable to determine the mode
case "$MODE" in
  OPERATOR)
    echo "Running as Operator..."
    kopf run OmeroDropboxOperator.py --verbose -n $(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
    ;;
  WEBHOOK)
    echo "Running as Webhook..."
    gunicorn --workers=${GUNICORN_WORKERS} --bind=0.0.0.0:8080 OmeroDropboxWebhook:app
    ;;
  WATCH)
    echo "Running as Watch..."
    # Ensure that WATCHED_DIR, WEBHOOK_URL, and WATCH_NAME are set
    if [ -z "$WATCHED_DIR" ] || [ -z "$WEBHOOK_URL" ] || [ -z "$WATCH_NAME" ]; then
        echo "The WATCHED_DIR, WATCH_NAME and WEBHOOK_URL environment variables must be set."
        echo "WATCHED_DIR=$WATCHED_DIR, WATCH_NAME=$WATCH_NAME, WEBHOOK_URL=$WEBHOOK_URL"
        exit 1
    fi
    
    # Optional: Regex pattern to ignore files
    IGNORE_PATTERN="${IGNORE_PATTERN:-}"
    
    should_ignore_file() {
        local file_path=$1
        if [[ $file_path =~ $IGNORE_PATTERN ]]; then
            return 0 # True, should ignore
        fi
        return 1 # False, should not ignore
    }
    
    # Function to send file path to the webhook
    send_to_webhook() {
        local file_path=$1
        if should_ignore_file "$file_path"; then
            echo "Ignoring file: $file_path"
            return
        fi
        curl -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" -d "{\"OmeroDropbox\":\"$WATCH_NAME\", \"fullPath\":\"$file_path\"}"
    }
    
    # Loop through existing files and send them to the webhook
    find "$WATCHED_DIR" -type f -print0 | while IFS= read -r -d $'\0' file; do
        send_to_webhook "$file"
    done
    
    # Start inotifywait to monitor new files and send them to the webhook
    inotifywait -m -r -e close_write --format '%w%f' "$WATCHED_DIR" | while read file; do
        send_to_webhook "$file"
    done
    ;;
  *)
    echo "No valid MODE specified. Please set MODE to OPERATOR, WEBHOOK, or WATCH."
    exit 1
    ;;
esac
