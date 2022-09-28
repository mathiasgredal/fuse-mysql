# Solution to OSDS exercise 7.3

## Setup
- Install system packages: `apt install python3 python3-pip python3-virtualenv pkg-config libfuse-dev`
- Create venv: `virtualenv venv`
- Activate venv: `source venv/bin/activate`
- Install python packages: `pip install -r requirements.txt`

## Run
- Start mysql database with: `docker run -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -v ${PWD}/db:/var/lib/mysql -it --rm mysql`
- Create mount directory `mkdir mnt`
- Run python script `python main.py ./mnt/`