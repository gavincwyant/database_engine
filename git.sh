#! /bin/bash

current_date = $("+%Y-%m-%d")

sed -i '' -e 'a\
*' README.md
git add .
git commit -m "Commit made on ${current_date}"
git push
