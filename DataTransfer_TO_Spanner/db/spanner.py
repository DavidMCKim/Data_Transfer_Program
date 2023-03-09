import pandas as pd
from google.cloud import spanner
from google.auth import compute_engine

class SpannerHelper():
    
    def __init__(self, instance_id, database_id, tousflux_key) -> None:
        self.spanner_client  = spanner.Client.from_service_account_json(tousflux_key)
        self.instance        = self.spanner_client.instance(instance_id)
        self.database        = self.instance.database(database_id)

    def SelectQuery(self, query):
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            lresults = list(results)

            collist = []
            for col in results.fields:
                collist.append(col.name)
            dfResult = pd.DataFrame(lresults, columns=collist)

        return dfResult



    def ExecuteQuery(self, query):
        """ INSERT / UPDATE / DELETE 쿼리 실행 """

        try:
            def exceuteData(transaction):
                row_ct = transaction.execute_update(query)
                return row_ct

            return self.database.run_in_transaction(exceuteData)

        except Exception as ex:
            return -1