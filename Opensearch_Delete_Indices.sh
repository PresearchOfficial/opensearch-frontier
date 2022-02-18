HOSTNAME=${1:-"http://localhost:9200"}
CREDENTIALS=${2:-"-u admin:admin"}

# delete existing indices

curl -XDELETE --insecure $CREDENTIALS "$HOSTNAME/status/"

curl -XDELETE --insecure $CREDENTIALS "$HOSTNAME/queues/"

curl -XDELETE --insecure $CREDENTIALS "$HOSTNAME/frontiers/"

curl -XDELETE --insecure $CREDENTIALS "$HOSTNAME/assignments/"

