#!/bin/bash

CRON_START_SCRIPT="$SCRIPT_DIR/cron_start_services.sh"

echo "Setting up cron job for RNA Plot services..."

if [ ! -f "$CRON_START_SCRIPT" ]; then
    echo "Error: $CRON_START_SCRIPT not found!"
    exit 1
fi

# Make sure the start script is executable
chmod +x "$CRON_START_SCRIPT"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "$CRON_START_SCRIPT"; then
    echo "Cron job already exists for RNA Plot services."
    echo "Current crontab:"
    crontab -l | grep "$CRON_START_SCRIPT"
else
    # Add the cron job
    (crontab -l 2>/dev/null; echo "@reboot $CRON_START_SCRIPT") | crontab -
    echo "Cron job added successfully!"
    echo "Services will now start automatically at boot."
fi

echo ""
echo "Current crontab entries related to RNA Plot:"
crontab -l 2>/dev/null | grep -E "(cron_start_services|stop_services)" || echo "No RNA Plot cron entries found."

echo ""
echo "To manually manage cron jobs:"
echo "  View:   crontab -l"
echo "  Edit:   crontab -e"
echo "  Remove: crontab -r"

echo ""
echo "To test the setup:"
echo "  Start:  $CRON_START_SCRIPT"
echo "  Stop:   $SCRIPT_DIR/stop_services.sh"
