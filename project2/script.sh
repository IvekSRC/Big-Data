case "$1" in
    docker-compose)
        parent_dir=$(dirname "$(pwd)")
        cd "$parent_dir" || exit
        docker-compose -f docker-compose-2.yaml up -d
        ;;
    docker-build)
        docker build --no-cache --rm -t bde/spark-app2 .
        ;;
    docker-run)
        docker run --name project2 --net bde -p 4040:4040 -d bde/spark-app2
        ;;
    producer-run)
        python3 producer.py
        ;;
    *)
        echo "Usage: $0 {docker-compose|docker-build|docker-run|producer-run}"
        exit 1
        ;;
esac

exit 0
