#!/bin/bash
if [ -z $GENV ]; then
	echo
	echo Python environment pointer GENV is not set. Looking for an environment at
	echo a few familiar places . . .
	echo
		for x in \
			~/venv ~/genv ~/gnana-env \
			~/workspace/venv ~/workspace/genv ~/workspace/gnana-env;
		do
		    echo Looking at $x . .
		    if [ -f $x/bin/activate ]; then
		        echo Found
		        export GENV=$x
		        break
		    fi
		done
fi

if [ -f $GENV/bin/activate ]; then
	source $GENV/bin/activate
	else
	echo
	echo No python environment found at: $GENV
	echo Exiting .
	exit 5
fi

export jsdebug=1

if [ -z $1 ]; then
	python manage.py shell
#elif [ $1 == 'celery' ]; then
#    shift
#    celery -A gnana $* 
elif [ -f local/$1 ]; then
	python manage.py run_local local/$1
else
	python manage.py $*
fi
