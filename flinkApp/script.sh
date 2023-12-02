case "$1" in
    docker-compose)
        parent_dir=$(dirname "$(pwd)")
        cd "$parent_dir" || exit
        docker-compose -f docker-compose-2-flink.yaml up -d
        ;;
    mvn-install)
        mvn clean install
        ;;
    producer-run)
        cd ..
        cd project2
        python3 producer-flink.py
        ;;
    *)
        echo "Usage: $0 {docker-compose|mvn-install|producer-run}"
        exit 1
        ;;
esac

exit 0
