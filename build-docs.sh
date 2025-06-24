echo " "
echo "About to build the documentation ..."

PACKAGE="sdmxabs"
cd ~/$PACKAGE
rm -rf ./docs
pdoc ./src/$PACKAGE -o ./docs 

