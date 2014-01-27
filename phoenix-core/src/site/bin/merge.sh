current_dir=$(cd $(dirname $0);pwd)
cd $current_dir
SITE_TARGET="../../../target/site"
java -jar merge.jar ../language_reference_source/index.html $SITE_TARGET/language/index.html
java -jar merge.jar ../language_reference_source/functions.html $SITE_TARGET/language/functions.html
java -jar merge.jar ../language_reference_source/datatypes.html $SITE_TARGET/language/datatypes.html
cd $SITE_TARGET

grep -rl class=\"nav-collapse\" . | xargs sed -i 's/class=\"nav-collapse\"/class=\"nav-collapse collapse\"/g';grep -rl class=\"active\" . | xargs sed -i 's/class=\"active\"/class=\"divider\"/g'
grep -rl "dropdown active" . | xargs sed -i 's/dropdown active/dropdown/g'
