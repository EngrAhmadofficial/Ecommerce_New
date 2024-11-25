#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the source and target repositories
SOURCE_REPO="/Users/ahmadmirza/elgrocer/Event Logging Kafka"
TARGET_REPO="/Users/ahmadmirza/Documents/Personal Github/Ecommerce_New"

# Define the paths of the files to copy
FILES_TO_COPY=(
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/adyen.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/epg.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/general.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/order.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/segment-marketing.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/shop-locations.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/smiles-non-transaction.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/smiles-transaction.consumer.ts"
  "apps/event-logging-service/src/module/consumer/consumer-group/analytics/utap.consumer.ts"
  "apps/event-logging-service/src/module/kafka/service/kafka.service.ts"
)

# Copy files from the source to the target repository
echo "Copying files from $SOURCE_REPO to $TARGET_REPO..."
for file in "${FILES_TO_COPY[@]}"; do
  # Ensure the target directory exists
  mkdir -p "$TARGET_REPO/$(dirname $file)"
  
  # Copy the file
  if [ -f "$SOURCE_REPO/$file" ]; then
    cp "$SOURCE_REPO/$file" "$TARGET_REPO/$file"
    echo "Copied: $file"
  else
    echo "Warning: File not found in source repo: $file"
  fi
done

# Navigate to the target repository
cd "$TARGET_REPO"

# Add the copied files to the git index
echo "Staging files in $TARGET_REPO..."
git add apps/

# Commit the files
echo "Committing the files..."
git commit -m "Populate files from elgrocer repository for patch application"

# Apply patches
PATCH_DIR="$SOURCE_REPO/filtered_patches"
echo "Applying patches from $PATCH_DIR..."
git am "$PATCH_DIR"/*.patch

if [ $? -eq 0 ]; then
  echo "Patches applied successfully."
else
  echo "Failed to apply patches. Please check and resolve conflicts."
  exit 1
fi
