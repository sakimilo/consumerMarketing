#!/bin/bash

#-----------------------------------------------------------------------------------
# Note that this is the approach of using
# conda environment.yml file for setting up required 
# environment for running the code in this project.
# The creation of this environment has been tested with Conda 4.7.10.
# However, this makes no assertions of proper working on another version of Conda.
#-----------------------------------------------------------------------------------
if [ -e environment.yml ]; then

    conda env create -f environment.yml

else

    # Create environment under name "assessment_LingYit"
    conda create --name assessment_LingYit --yes

    # Activate this conda environment
    conda activate assessment_LingYit

    # Install pip into the current conda environment
    conda install pip --yes

    ### Essentials
    pip install pandas
    pip install numpy
    pip install scipy
    pip install cx_Oracle
    pip install python-logstash
    
    ### Charting libraries
    pip install matplotlib
    pip install seaborn

    ### Utilities
    pip install tqdm
    pip install jsonref

    ### Spark-related
    pip install pyarrow

    ### Documentation creation tools
    pip install sphinx
    pip install sphinx_rtd_theme

    ### Database stuff
    pip install psycopg2-binary

    ### Export environment info to YML
    conda env export > environment.yml

    conda deactivate
fi