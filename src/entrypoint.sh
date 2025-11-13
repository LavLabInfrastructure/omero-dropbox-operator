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

    # Track file sizes from the previous poll to detect stability.
    declare -A PREV_SIZES

    while true; do
        declare -A NEXT_SIZES

        # Use process substitution to keep the loop in the same shell (so arrays persist).
        while IFS= read -r -d $'\0' file; do
            # skip directories (find -type f should avoid these, but keep the check just in case)
            if [[ -d "$file" ]]; then
                continue
            fi

            # Get current file size; if stat fails, skip.
            size=$(stat -c%s "$file" 2>/dev/null || echo -1)
            if [[ "$size" -lt 0 ]]; then
                continue
            fi

            prev_size=${PREV_SIZES["$file"]}
            if [[ -n "$prev_size" && "$prev_size" -eq "$size" ]]; then
                echo "Stable file (unchanged for $POLL_INTERVAL s): $file"
                send_to_webhook "$file"
            fi

            NEXT_SIZES["$file"]="$size"
        done < <(find "$WATCHED_DIR" -type f -print0)

        # Prepare for next poll: replace PREV_SIZES with NEXT_SIZES
        PREV_SIZES=()
        for k in "${!NEXT_SIZES[@]}"; do
            PREV_SIZES["$k"]="${NEXT_SIZES["$k"]}"
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
