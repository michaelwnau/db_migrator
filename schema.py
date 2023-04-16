import os
import json
from graphene import Mutation

# Load the JSON file containing the connection parameters
data_file_path = 'data.json'
if os.path.exists(data_file_path):
    with open(data_file_path) as data_file:
        data = json.load(data_file)
else:
    data = {}


# Define the classes, queries, and schema that originated in the originary database to be migrated
# ...

# Define the input types for the mutation
class TableNamesInput(graphene.InputObjectType):
    column1 = graphene.String()
    column2 = graphene.String()
    # ... and so on.

# Define the mutation class for the GraphQL schema


class CreateData(Mutation):
    class Arguments:
        input = TableNameInput(required=True)

    success = Boolean()
    data = Field(lambda: TableName)

    def mutate(root, info, input):
        global data
        new_data = {
            "column1": input.column1,
            "column2": input.column2,
            # Add more columns as needed
        }
        data.append(new_data)
        # Save the updated data to the JSON file
        with open(data_file_path, "w") as file:
            json.dump(data, file)
        return CreateData(success=True, data=new_data)


class Mutation(ObjectType):
    create_data = CreateData.Field()


# Update the schema to include the mutations
schema = graphene.Schema(query=Query, mutation=Mutation)

# Replace TableName and the column names with the names from the JSON data.
