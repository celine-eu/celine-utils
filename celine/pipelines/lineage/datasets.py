def make_dataset(namespace: str, name: str, schema=None, stats=None):
    facets = {}
    if schema:
        facets["schema"] = {"fields": list(schema.keys())}
    if stats:
        facets["outputStatistics"] = stats
    return {"namespace": namespace, "name": name, "facets": facets}
