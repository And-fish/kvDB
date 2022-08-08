rm -rf work_test
git config --global --unset http.proxy
git add -A
git commit -m $1
git push origin master
