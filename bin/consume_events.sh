#!/bin/bash

# Shell script to support system service to continuously run stop event data pipeline consumer
# The following tutorial was used as a reference for the outline of this script: https://ubuntudoc.com/how-to-create-new-service-with-systemd/

consumer_start()
{
	echo "Kafka Stop Event Consumer: Starting Service"
	nohup /home/werner/confluent-env/bin/python3 /home/werner/consume_events.py > /dev/null 2>&1 &
}

consumer_stop()
{
	echo "Kafka Stop Event Consumer: Stopping Service"
	ps -ef | grep consume_events.py | grep -v grep | awk '{print $2}' | xargs sudo kill
}

consumer_status()
{	
	if ps -ef | grep consume_events.py | grep -v grep ; then 
		echo "Stop Event Consumer is running"
	else
		echo "Stop Event Consumer is not running"
	fi
}

case "$1" in
	start)
		consumer_start
		;;
	stop)
		consumer_stop
		;;
	reload)
		consumer_stop
		sleep 1
		consumer_start
		;;
	status)
		consumer_status
		;;
	*)
		echo "Usage: $0 {start | stop | reload | status}"
		exit 1
		;;
esac

exit 0
		
