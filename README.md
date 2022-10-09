# SEC

Running data retrievers:

First; create a `.env` file that contains a variable named `email` and an email address to include along with requests to the SEC. On a Linux/Unix system, you can create a properly formatted `.env` file via the command below (with the example email replaced with a real one).

```bash
user@host:~/.../sec$ echo "email=example.email@email.com" > .env
```

Add a mongo_db password and username via

```bash
user@host:~/.../sec$ mkdir secrets
user@host:~/.../sec$ echo "your_mongo_user_name" > secrets/mongo_user.txt
user@host:~/.../sec$ echo "your_mongo_password" > secrets/mongo_password.txt
```

And you can spin up a mongo-db database via 

```bash
user@host:~/.../sec$ docker-compose up
```

## DERA Data Library

The Division of Economic and Risk Analysis (DERA) offers investors and market participants access to aggregated data from public filings for research and analysis.

* [links](https://www.sec.gov/dera/data)

## EDGAR

* [Developer resources](https://www.sec.gov/developer)
* [Technical documentation](https://www.sec.gov/info/edgar/specifications/pds_dissemination_spec.pdf)
* [api docs](https://www.sec.gov/edgar/sec-api-documentation)
* [Form types](https://www.sec.gov/oiea/Article/edgarguide.html)

* Data Sources:
    * [feed](https://www.sec.gov/Archives/edgar/Feed/)
    * [full-index](https://www.sec.gov/Archives/edgar/full-index/)
    * [daily-index](https://www.sec.gov/Archives/edgar/daily-index/)

* ILSOS (Not related, but I just stumbled across it)
    * https://www.ilsos.gov/data/bus_serv_home.html