FROM apache/airflow:2.5.0-python3.10

COPY requirements ./requirements

# Install pandas for Airflow 2.4 example
RUN pip install --upgrade pip
RUN pip install -r ./requirements/requirements.txt

# Install for LocalKubernetess Executor example for Airflow 2.3
RUN pip install apache-airflow[cncf.kubernetes]

# Required to install package for an Airflow 2.2 example
RUN mkdir -p ./package_folder/src/feat22/

# SETUP STRUCTURE FOR INSTALLING PACKAGES
COPY dags/feat22/mypkg ./package_folder/src/feat22/mypkg
COPY dags/feat22/*.py ./package_folder/src/feat22

COPY setup.py ./package_folder/setup.py

RUN cd ./package_folder && python ./setup.py bdist_wheel

RUN cd ./package_folder && pip install .

# test that the package is installed
RUN python -c "import feat22; help(feat22)"
