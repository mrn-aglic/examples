FROM apache/airflow:2.4.2-python3.10

COPY requirements ./requirements

RUN pip install --upgrade pip
RUN pip install -r ./requirements/requirements.txt

RUN mkdir -p ./package_folder/src/feat22/

# SETUP STRUCTURE FOR INSTALLING PACKAGES
COPY dags/feat22/mypkg ./package_folder/src/feat22/mypkg
COPY dags/feat22/*.py ./package_folder/src/feat22

COPY setup.py ./package_folder/setup.py

RUN cd ./package_folder && python ./setup.py bdist_wheel

RUN cd ./package_folder && pip install .

# test that the package is installed
RUN python -c "import feat22; help(feat22)"
