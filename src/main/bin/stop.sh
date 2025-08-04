# This program is free software;
# you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program;
# if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#!/bin/bash

cygwin=false;
linux=false;
case "`uname`" in
    CYGWIN*)
        bin_abs_path=`cd $(dirname $0); pwd`
        cygwin=true
        ;;
    Linux*)
        bin_abs_path=$(readlink -f $(dirname $0))
        linux=true
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac

search_pid() {
    STR=$1
    PID=$2
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
        if $linux; then
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else
                JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        else
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps aux |grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else
                JAVA_PID=`ps aux |grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        fi
    fi
    echo $JAVA_PID;
}

base=${bin_abs_path}/..
name="appName=datachecker"
pidfile=$base/bin/datachecker.pid
if [ ! -f "$pidfile" ];then
	echo "datachecker is not running. so exists"
	exit
fi

if [ ! -f "$pidfile" ];then
    pid=`cat $pidfile`
fi

if [ "$pid" == "" ] ; then
   pid=`search_pid "appName=datachecker"`
fi

echo -e "`hostname`: stopping datachecker $pid ... "
kill $pid

cost=0
timeout=30
while [ $timeout -gt 0 ]; do
	gpid=`search_pid "appName=datachecker" "$pid"`
    if [ "$gpid" == "" ] ; then
    	echo "Oook! cost:$cost"
    	`rm $pidfile`
    	break;
    fi
    let LOOPS=LOOPS+1
    sleep 1
    let timeout=timeout-1
    let cost=cost+1
done

if [ $timeout -eq 0 ] ; then
    kill -9 $pid
    gpid=`search_pid "appName=datachecker" "$pid"`
    if [ "$gpid" == "" ] ; then
        echo "Oook! cost:$cost"
        `rm $pidfile`
        break;
    else
        echo "Check kill pid ${pid} failed."
        exit 1
    fi
fi
