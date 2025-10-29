find . -maxdepth 1 -type d -printf "%f\n" | grep -E '^[0-9a-f]{32}$' | xargs -d '\n' rm -r
