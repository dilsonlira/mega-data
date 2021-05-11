db_container="mega-data_db_1"
db_name="mega"
db_user="root"
db_password=$(cat db/password.txt)
cmd="docker exec -it $db_container mysql $db_name -u$db_user -p$db_password"
if [ "$1" = t ]; then
    query="show databases;show tables"
    qcmd="$cmd -e '$query'"
elif [ "$1" = q ]; then
    query="${@:2}"
    qcmd="$cmd -e '$query'"
else
    qcmd=$cmd
fi
eval $qcmd
