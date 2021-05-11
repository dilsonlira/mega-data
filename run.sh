if flake8; then
    if mypy .; then
        if test -f docker-compose.yaml; then
            docker-compose down -v
            docker-compose build
            if [ "$1" = db ]; then
                if docker compose up | grep 'Database load completed' -m1; then
                    ./db.sh
                fi
            else
                docker compose up
            fi
        else
            echo "There is no docker-compose.yaml in the current directory."
        fi
    fi
fi
