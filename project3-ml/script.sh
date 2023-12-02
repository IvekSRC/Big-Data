case "$1" in
    docker-compose)
        parent_dir=$(dirname "$(pwd)")
        cd "$parent_dir" || exit
        docker-compose -f docker-compose-3.yaml up -d
        ;;
    docker-build)
        docker build --rm -t bde/classification-app .
        ;;
    docker-run)
        docker run --name project3-classification --net bde -p 4040:4040 -d bde/classification-app
        ;;
    start-producer)
        python3 producer.py
        ;;
    *)
        echo "Usage: $0 {docker-compose|docker-build|docker-run|start-producer}"
        exit 1
        ;;
esac

exit 0
