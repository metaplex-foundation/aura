#!/usr/bin/env bash
set +e

VERSION="$(git describe --tags --abbrev=0 2&> /dev/null || echo '0.1.0')-$(git rev-parse --short HEAD)"

echo "
VERSION:              $VERSION
BUILD TIMESTAMP:      $(date -u +"%a %b %d %T UTC %Y")
COMMIT MESSAGE:       $(git log -1 --pretty=%B | tr '\n' ' ')
COMMIT AUTHOR:        $(git log -1 --pretty=format:'%an <%ae>')
COMMIT HASH:          $(git rev-parse HEAD)
MERGE REQUEST TITLE:  $(git log -1 --pretty=%s)
REPOSITORY HOMEPAGE:  $(git config --get remote.origin.url)
"
