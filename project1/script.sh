case "$1" in
    docker-compose)
        parent_dir=$(dirname "$(pwd)")
        cd "$parent_dir" || exit
        docker-compose -f docker-compose.yaml up -d
        ;;
    docker-build)
        docker build --rm -t bde/spark-app .
        ;;
    docker-run)
        docker run --name project1 --net bde -p 4040:4040 -d bde/spark-app
        ;;
    *)
        echo "Usage: $0 {docker-compose|docker-build|docker-run|local-run}"
        exit 1
        ;;
esac

exit 0
