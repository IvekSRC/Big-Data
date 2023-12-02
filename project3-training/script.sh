case "$1" in
    docker-compose)
        parent_dir=$(dirname "$(pwd)")
        cd "$parent_dir" || exit
        docker-compose -f docker-compose.yaml up -d
        ;;
    docker-build)
        docker build --rm -t bde/training-app .
        ;;
    docker-run)
        docker run --name project3-training --net bde -p 4040:4040 -d bde/training-app
        ;;
    *)
        echo "Usage: $0 {docker-compose|docker-build|docker-run}"
        exit 1
        ;;
esac

exit 0
