#!/bin/bash

# Check environment variable to determine the mode
case "$MODE" in
  OPERATOR)
    echo "Running as Operator..."
    kopf run OmeroDropboxOperator.py --verbose -n $(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
    ;;
  WEBHOOK)
    echo "Running as Webhook..."
    python3 OmeroDropboxWebhook.py
    ;;
  WATCH)
    echo "Running as Watch..."
    if [[ ! -d '/watch' ]]; then
        echo "The /watch directory does not exist. Please mount a volume to /watch."
        exit 1
    fi
    
    echo "Waiting for the scheduler to be ready..."
    while ! curl -s "http://localhost:8080/ready" > /dev/null; do
        echo "Scheduler is not ready yet. Waiting..."
        sleep 5
    done
    echo "Scheduler is ready."

    
    mkdir -p $WATCHED_DIR
    
    # Optional: Regex pattern to ignore files
    IGNORE_PATTERN="${IGNORE_PATTERN:-\.txt}"
    
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
        curl -X POST "http://localhost:8080/import" -H "Content-Type: application/json" -d "{\"OmeroDropbox\":\"$WATCH_NAME\", \"fullPath\":\"$file_path\"}"
    }
    
    # Poll the watched directory periodically and send all files to the webhook.
    # This replaces inotifywait with a simple polling loop which runs every
    # POLL_INTERVAL seconds (default 60). The webhook should deduplicate or
    # avoid scheduling duplicates (per repository behaviour).
    POLL_INTERVAL="${POLL_INTERVAL:-60}"

    while true; do
        find "$WATCHED_DIR" -type f -print0 | while IFS= read -r -d $'\0' file; do
            # skip directories (find -type f should avoid these, but keep the
            # check just in case)
            if [[ -d "$file" ]]; then
                continue
            fi
            echo "Polling detected file: $file"
            send_to_webhook "$file"
        done

        # Sleep before the next poll
        sleep "$POLL_INTERVAL"
    done
    ;;
  *)
    echo "No valid MODE specified. Please set MODE to OPERATOR, WEBHOOK, or WATCH."
    exit 1
    ;;
esac
