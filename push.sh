git config --global --unset https.proxy
git add -A
git commit -m $1
git push origin master
