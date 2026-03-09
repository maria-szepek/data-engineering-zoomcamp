# prerequisites

* i installed java version 21.xxx using sdkman: 

sdk update 
sdk list java 
sdk use java 21.0.10-tem / sdk default java 21.0.10-tem
java --version + echo $JAVA_HOME / readlink -f $JAVA_HOME (there is a simlink from "current" to actual version) to confirm

* i did not install pyspark properly, instead i did only use uv version manager to add pyspark

in this directory here: 
uv init 
uv add pyspark 
uv add jupyter 

in ./notebooks: 
jupyter notebook 

inside the notebook: 
import pyspark
pyspark.__version__
pyspark.__file__

... somehow worked. 

* created a test script (downloads and writes to local filesystem a csv file with spark)

* INTERFACE FOR SPARK MASTER: localhost:4040

* if executed on remote machine, port forwarding needed for jupyter (8888) and spark master (4040)

* .gitignore: added for spark output: 

_SUCCESS
*.crc






