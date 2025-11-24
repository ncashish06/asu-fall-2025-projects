from neo4j import GraphDatabase


class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        # TODO: Implement this method
        with self._driver.session() as session:
            try:
                # Cleaning any existing projection
                session.run("CALL gds.graph.drop('bfsGraph', false)").consume()

                # Projecting the graph (default orientation: NATURAL)
                session.run(
                    """
                CALL gds.graph.project(
                'bfsGraph',
                'Location',
                'TRIP'
                )
                """
                ).consume()

                result = session.run(
                    """
                MATCH (source:Location {name: $start}),
                    (target:Location {name: $end})
                CALL gds.shortestPath.dijkstra.stream('bfsGraph', {
                sourceNode: source,
                targetNode: target
                })
                YIELD nodeIds
                RETURN [nid IN nodeIds | {name: toInteger(gds.util.asNode(nid).name)}] AS path
                """,
                    start=int(start_node),
                    end=int(last_node),
                )

                bfs_ouput = result.data()

                # Cleaning up the projection
                session.run("CALL gds.graph.drop('bfsGraph', false)").consume()

                return bfs_ouput

            except Exception as e:
                print("Error occurred while executing BFS:", e)
                return []

    def pagerank(self, max_iterations, weight_property):
        # TODO: Implement this method
        with self._driver.session() as session:
            try:
                iters = int(max_iterations)
                weight_prop = str(weight_property)

                # Cleaning any existing projection
                session.run("CALL gds.graph.drop('prGraph', false)").consume()

                # Projecting the graph
                session.run(
                    """
                CALL gds.graph.project(
                  'prGraph',
                  'Location',
                  { 
                    TRIP: { 
                        type: 'TRIP',
                        orientation: 'NATURAL',
                        properties: $weight_prop
                    } 
                  }
                )
                """,
                    weight_prop=weight_prop,
                ).consume()

                # Scores
                rows = list(
                    session.run(
                        """
                    CALL gds.pageRank.stream('prGraph', { maxIterations: $iters, dampingFactor: 0.85, relationshipWeightProperty: $weight_prop })
                    YIELD nodeId, score
                    WITH gds.util.asNode(nodeId) AS n, score
                    RETURN toInteger(n.name) AS name, score
                    ORDER BY score DESC
                """,
                        iters=iters,
                        weight_prop=weight_prop,
                    )
                )

                # Cleaing up the projection
                session.run("CALL gds.graph.drop('prGraph', false)").consume()
                if not rows:
                    return []

                max_node = {"name": rows[0]["name"], "score": float(rows[0]["score"])}
                min_row = rows[-1]
                min_node = {"name": min_row["name"], "score": float(min_row["score"])}
                return [max_node, min_node]

            except Exception as e:
                print("Error in PageRank:", e)
                return []
