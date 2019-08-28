# Install PostgreSQL
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -sc)-pgdg main" > /etc/apt/sources.list.d/PostgreSQL.list'
sudo apt-get update
sudo apt-get -y install postgresql-10

# Mount disk
sudo mkdir -p /mnt/disks/disk-1
sudo mount -o discard,defaults /dev/sdb /mnt/disks/disk-1
sudo chmod a+w /mnt/disks/disk-1


# Init database
sudo -u postgres psql -c "ALTER ROLE postgres WITH PASSWORD '12345';
CREATE SCHEMA olap;

CREATE TABLE olap.calendar (
    date date NOT NULL,
    week_date date NOT NULL,
    month_date date NOT NULL,
    year_date date NOT NULL,
    PRIMARY KEY (date)
);

CREATE TABLE olap.stores (
    store_id INT NOT NULL,
    store_type TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    has_breakfast INT NOT NULL,
    has_dt INT NOT NULL,
    PRIMARY KEY (store_id)
);

CREATE TABLE olap.menu_items (
    menu_item_id INT NOT NULL,
    cat1 TEXT NOT NULL,
    cat2 TEXT,
    cat3 TEXT NOT NULL,
    PRIMARY KEY (menu_item_id)
);

CREATE TABLE olap.sales (
    date date NOT NULL,
    store_id INT NOT NULL,
    menu_item_id INT NOT NULL,
    units INT NOT NULL,
    sales float8 NOT NULL
);"

sudo -u postgres psql -c "COPY olap.stores 
FROM '/mnt/disks/disk-1/olap/stores.txt' 
CSV delimiter E'\t' HEADER;"

sudo -u postgres psql -c "COPY olap.menu_items 
FROM '/mnt/disks/disk-1/olap/menu_items.txt' 
CSV delimiter E'\t' HEADER;"

sudo -u postgres psql -c "COPY olap.calendar 
FROM '/mnt/disks/disk-1/olap/calendar.txt' 
CSV delimiter E'\t' HEADER;"

time sudo -u postgres psql -c "COPY olap.sales 
FROM '/mnt/disks/disk-1/olap/sales.txt' 
CSV delimiter E'\t' HEADER;"

touch ~/test.sh
