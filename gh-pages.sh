#!/bin/sh -euv

echo "Creating site"
sbt -Dsbt.global.base=$TRAVIS_BUILD_DIR/ci make-site

git config --global user.email "omnia-bamboo"
git config --global user.name "Travis"

echo "Cloning gh-pages"
git clone -b gh-pages https://omnia-bamboo:$GH_PASSWORD@github.com/$TRAVIS_REPO_SLUG.git ./target/gh-pages

cd ./target/gh-pages
git rm -r -f --ignore-unmatch *
cp -r ../site/* .

echo "releaseVersion: `grep VERSION ../VERSION.txt | cut -d '=' -f 2`" >> _config.yml

git add .
git commit -m "Updated site"
echo "Pushing site to gh-pages"
git push --quiet
cd ..
rm -rf gh-pages
