#!/bin/sh

appname=""
for i in app.yaml ../appengine/app.yaml
do
	if [ -r $i ]
	then
		appname=`grep -e '^application:' $i | cut -d ' ' -f 2`
		break
	fi
done

if [ -z "$appname" ]
then
	echo -n "Cannot retrieve appname, please enter: "
	read appname

	[[ -z "$appname" ]] && exit 1
fi

key="`openssl rand -base64 64 | md5sum | awk '{ print $1 }'`"

if [ "$key" == "d41d8cd98f00b204e9800998ecf8427e" ] # Prevent empty md5 hash if openssl is unavailable
then
	echo "Key generation failed"
	exit 1
fi

echo
echo "${appname}.appspot.com:$key"

cat <<ENDL >key.py
# Generated `date` by `whoami`
# to regenerate, run $0
backupKey = u'$key'
ENDL

sed -e "s/APPNAME/$appname/g" app-template.yaml >app.yaml
