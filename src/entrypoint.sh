#!/bin/bash

# Check environment variable to determine the mode
case "$MODE" in
  OPERATOR)
    echo "Running as Operator..."
    kopf run OmeroDropboxOperator.py --verbose -n "$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)"
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

    # Default watched dir (can be overridden through env)
    WATCHED_DIR="${WATCHED_DIR:-/watch}"
    # If true, follow symlinks when scanning (set FOLLOW_SYMLINKS=true)
    FOLLOW_SYMLINKS="${FOLLOW_SYMLINKS:-false}"

    echo "Waiting for the scheduler to be ready..."
    while ! curl -s "http://localhost:8080/ready" > /dev/null; do
        echo "Scheduler is not ready yet. Waiting..."
        sleep 5
    done
    echo "Scheduler is ready."

    mkdir -p "$WATCHED_DIR"

    # Optional: Regex pattern to ignore files (applies to full path)
    IGNORE_PATTERN=${IGNORE_PATTERN:-'/\.[^./][^/]*'}

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
        curl -s -X POST "http://localhost:8080/import" -H "Content-Type: application/json" -d "{\"OmeroDropbox\":\"$WATCH_NAME\", \"fullPath\":\"$file_path\"}" >/dev/null || \
            echo "Warning: failed to POST $file_path"
    }

    # Poll the watched directory periodically and send stable files to the webhook.
    # POLL_INTERVAL seconds between scans (default 60)
    POLL_INTERVAL="${POLL_INTERVAL:-60}"

    # Track file sizes from the previous poll to detect stability.
    declare -A PREV_SIZES

    # Build find options (use -L to follow symlinks when requested)
    if [[ "${FOLLOW_SYMLINKS}" == "true" || "${FOLLOW_SYMLINKS}" == "1" ]]; then
        FIND_OPTS=(-L)
        echo "Scanning $WATCHED_DIR recursively (following symlinks)"
    else
        FIND_OPTS=()
        echo "Scanning $WATCHED_DIR recursively (not following symlinks)"
    fi

    while true; do
        declare -A NEXT_SIZES

        # show scan start so we know it's running
        echo "Starting scan at $(date)"

        # Use process substitution so the while loop runs in the current shell
        # (not in a subshell) and NEXT_SIZES updates are visible afterwards.
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
        done < <(find "${FIND_OPTS[@]}" "$WATCHED_DIR" -type f -print0 2>/dev/null)

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
