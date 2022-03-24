from vespa.package import ApplicationPackage
from vespa.package import Field
from vespa.package import FieldSet
from vespa.deployment import VespaDocker
import pandas as pd


def type_changer(val):
    return str(val)

df = pd.read_csv('drugsComTrain_raw.csv')

df["uniqueID"] = df['uniqueID'].apply(type_changer)
df['date'] = df['date'].apply(type_changer)
df = df.dropna()

app_package = ApplicationPackage(name="drugs")

app_package.schema.add_fields(
    Field(
        name = "uniqueID",
        type = "string",
        indexing = ["attribute", "summary"],
        attribute=["fast-search"]
    ),
    Field(
        name = "drugName",
        type = "string",
        indexing = ["index", "summary"]
        
    ),
    Field(
        name = "condition",
        type = "string",
        indexing = ["index", "summary"],        
    ),
    Field(
        name = "review",
        type = "string",
        indexing = ["index", "summary"]
        
    ),
    Field(
        name = "rating",
        type = "int",
        indexing = ["attribute", "index"]
        
    ),
    Field(
        name = "date",
        type = "string",
        indexing = ["index", "summary"]
        
    ),
    Field(
        name = "usefulCount",
        type = "int",
        indexing = ["index", "attribute"]
        
    ),
    Field(
        name="isalive",
        type="string",
        indexing = ["index", "attribute"]
    )
)


vespa_docker = VespaDocker(port=8080,container_memory='8G',disk_folder='/home/yashodhara/Internship/vespa/docker/')

app = vespa_docker.deploy(
    application_package = app_package,
)

for idx, row in df.iterrows():
    
    field = {
        "uniqueID" : row["uniqueID"],
        "drugName" : row["drugName"],
        "condition" : row["condition"],
        "review" : row["review"],
        "rating" : row["rating"],
        "date" : row["date"],
        "usefulCount" : row["usefulCount"]
    }

    response = app.feed_data_point(
        schema = "drugs",
        data_id = str(row["uniqueID"]),
        fields = field
    )
    if idx%1000==0:
        print(response.json)



