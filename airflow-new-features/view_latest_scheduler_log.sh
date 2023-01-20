#!/bins/sh

for file in $(find . -type f -iname ".env.backup");
do
  target=${file%.backup}

  echo "Copying $file to $target"

  cp $file $target
done;
