ZIP_FILE = submission.zip
ZIP_TMP_DIR = /tmp/15799_$(ZIP_FILE)_tmp/
SOURCE_DIR = .

all: submit

submit:
	sed -i 's/8096/5096/g' optimize.sh
	sed -i 's/n_runs = 5/n_runs = 1/g' ./calcite_app/src/main/java/edu/cmu/cs/db/calcite_app/app/App.java
	rm -rf $(ZIP_TMP_DIR)
	rm -f $(ZIP_FILE)
	rsync -av --exclude-from=$(SOURCE_DIR)/.gitignore --exclude-from=$(SOURCE_DIR)/calcite_app/.gitignore --exclude '.git' $(SOURCE_DIR)/ $(ZIP_TMP_DIR)
	cp calcite_app/gradle/wrapper/gradle-wrapper.jar $(ZIP_TMP_DIR)/calcite_app/gradle/wrapper/gradle-wrapper.jar
	cd $(ZIP_TMP_DIR) && zip -r $(ZIP_FILE) .
	mv $(ZIP_TMP_DIR)/$(ZIP_FILE) $(ZIP_FILE)
	rm -rf $(ZIP_TMP_DIR)
	sed -i 's/5096/8096/g' optimize.sh
	sed -i 's/n_runs = 1/n_runs = 5/g' ./calcite_app/src/main/java/edu/cmu/cs/db/calcite_app/app/App.java

clean:
	rm -f $(ZIP_FILE)
