using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Newtonsoft;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using AutoMapper.Mappers;
using AutoMapper;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Threading.Tasks;
using System.Net;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using System.Collections;

namespace convertCDMSchemaToGraph
{
    class Program
    {
        private static int EntityCount;
        private const string hostName = "cdmgraph.gremlin.cosmos.azure.com";
        private const int port = 443;
        private const string EndpointUrl = "https://cdmgraph.documents.azure.com:443/";
        private const string PrimaryKey = "4f7b831e-cd72-48d5-87be-4e6e9c69a807";
        private const string DBName = "CDMDB";
        private const string CollectionName = "cdmGraphCollection";
        private DocumentClient client;
        private List<Entity> allEntities = new List<Entity>();
        public Dictionary<string, string> gremlinQueries = new Dictionary<string,string>();

        static void Main(string[] args)
        {

            Program p = new Program();
            Mapper.Initialize(cfg => { });
            FilterAndRead("D:/CDM/CDM/schemaDocuments",p.allEntities); //path to local file
            

            //create dictionary to write via GremlinQuery
            p.CreateGremlinUploadDict(p.gremlinQueries,p.allEntities);
            
            
            //create gremlinserver
            try
            {
                p.setupDB().Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine($"{de.StatusCode} error occurred: {de.Message}, Message: {baseException.Message}");
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine($"Error: {e.Message}, Message: {baseException.Message}");
            }
            finally
            {
                Console.WriteLine("!!!! Summary !!!!");
                foreach(Entity ent in p.allEntities)
                {
                    int countAttributes = ent.attributeList.Count();
                    int countDep = ent.dependencyList.Count();
                    Console.WriteLine(" {0} has {1} attributes an {2} dependencies", ent.getEntityName(), countAttributes, countDep);
                }
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
            
        }

        public class ConsoleSpinner
        {
            int counter;

            public void Turn()
            {
                counter++;
                switch (counter % 4)
                {
                    case 0: Console.Write("/"); counter = 0; break;
                    case 1: Console.Write("-"); break;
                    case 2: Console.Write("\\"); break;
                    case 3: Console.Write("|"); break;
                }
                Console.SetCursorPosition(Console.CursorLeft - 1, Console.CursorTop);
            }
        }
            /* connect to DB
             */
            private async Task setupDB()
        {

            var gremlinServer = new GremlinServer(hostName, port, enableSsl: true,
                                    username: "/dbs/" + DBName + "/colls/" + CollectionName,
                                    password: PrimaryKey);
            
            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
            {
                foreach (var query in this.gremlinQueries)
                {
                    try
                    {
                        
                        var result = await gremlinClient.SubmitAsync<dynamic>(query.Value);
                        Console.WriteLine(query.Value);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error: {ex.Message}");
                    }
                }
            }
        }

        /* Read schema files without 0.7, 0.8 versions */
        static void FilterAndRead(string dir,List<Entity> entList)
        {
            try
            {
                foreach (string f in Directory.GetFiles(dir))
                {
                    if (Path.GetExtension(f).Contains("json") && !(Path.GetFileName(f).Contains("0.")))
                    {
                        Console.WriteLine(f + "  FileName is : " + Path.GetFileName(f));
                        try
                        {
                            LoadSchemaFile(f, entList);
                        } catch(System.Exception ex)
                        {
                            Console.WriteLine("{0} : Exception {1} ", f, ex.Message); 
                        }
                        Console.WriteLine("Loaded " + Program.EntityCount + " entities\n");
                    }
                }
                    
                foreach (string d in Directory.GetDirectories(dir))
                {
                    FilterAndRead(d,entList);
                }
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }


        public static void LoadSchemaFile(string filePath,List<Entity> allEntities)
        {
            //int count = 0;
            using (System.IO.StreamReader r = new StreamReader(filePath))
            {
                String json = r.ReadToEnd();
                var converter = new ExpandoObjectConverter();
                dynamic jsonmessage = JsonConvert.DeserializeObject<ExpandoObject>(json,converter);
                JToken token = JToken.Parse(json);
                Entity objEntity = new Entity();

                foreach (var tempVal in jsonmessage)
                {
                    if(tempVal.Key is "definitions")
                    {
                        foreach (var temp2 in tempVal.Value)
                        {
                            foreach (var tempAttr in temp2)
                            {
                                if (tempAttr.Key is "entityName")
                                {
                                    objEntity.setEntityName(tempAttr.Value.ToString());
                                    objEntity.setEntityId(tempAttr.Value.ToString());
                                }
                                if (tempAttr.Key is "extendsEntity")
                                {
                                    objEntity.setParentEntityName(tempAttr.Value.ToString());
                                }
                                if (tempAttr.Key is "hasAttributes")
                                {
                                    //cast to the attribute class
                                    foreach (var tempMember in tempAttr.Value[0])
                                    {
                                        Console.WriteLine(tempMember.Value.GetType().Name);
                                        foreach (KeyValuePair<string, object> kvp in ((IDictionary<string, object>)tempMember.Value))
                                        {
                                            //string PropertyWithValue = kvp.Key + ": " + kvp.Value.ToString();
                                            if (kvp.Key is "members")
                                            {
                                                //use automapper to map the attributes of member segment
                                                foreach (var temp in ((List<object>)kvp.Value))
                                                {
                                                    RawAttribute result = Mapper.Map<RawAttribute>(temp);
                                                    //Extract Dependencies
                                                    objEntity.dependencyList.AddRange(ExtractDependency(result));                                                 
                                                    if (result.displayName is null || result.dataType is null)
                                                    {
                                                        //Console.WriteLine("{0} : {1}", result.displayName, result.dataType);
                                                    }
                                                    else
                                                    {
                                                        objEntity.attributeList.Add(result);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (objEntity.getEntityName() != String.Empty)
                {
                    allEntities.Add(objEntity);
                    Program.EntityCount++;
                }
            }
        }

        public void CreateGremlinUploadDict(Dictionary<string, string> gremlinQueries, List<Entity> entityList)
        {
            //first create query to add nodes
            //syntax :  { "Cleanup",        "g.V().drop()" },
            //gremlinQueries.Add("Cleanup", "g.V().drop()");
            string startNode = "g.addV('entity').property('id','";
            string fillerPropertyEnd = "')";
            string fillerPropertyStart = ".property('";
            int edgeIndex = 1, vertexIndex = 1;
            //public List<RawAttribute> attributeList;
            //public List<InnerDependency> dependencyList;

            foreach (var tmpEntity in entityList)
            {
                //"AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
                //Add Entity
                string queryString = startNode + tmpEntity.Id + fillerPropertyEnd
                                    + fillerPropertyStart + "entityName','" + tmpEntity.getEntityName() + fillerPropertyEnd
                                    + fillerPropertyStart + "entityType','Entity" + fillerPropertyEnd;
                string vertexKey = "AddVertex " + vertexIndex.ToString();
                vertexIndex++;
                gremlinQueries.Add(vertexKey, queryString);
                Console.WriteLine(vertexKey + " , " + queryString);

                //Add Parent
                string queryStringParent = startNode + tmpEntity.getParentEntityName() + fillerPropertyEnd
                                    + fillerPropertyStart + "entityName','" + tmpEntity.getParentEntityName() + fillerPropertyEnd;
                string vertexKeyParent = "AddVertex " + vertexIndex.ToString();
                vertexIndex++;
                gremlinQueries.Add(vertexKeyParent, queryStringParent);
                Console.WriteLine(vertexKeyParent + " , " + queryStringParent);
            }
            foreach (var tmpEntity in entityList)
            {                
                //add attributes of this entity also
                foreach (RawAttribute rawAttr in tmpEntity.attributeList)
                {
                    string queryStringAttr = startNode + rawAttr.name + fillerPropertyEnd
                                            + fillerPropertyStart + "entityName','" + rawAttr.name + fillerPropertyEnd
                                            + fillerPropertyStart + "dataType','" + rawAttr.displayName + fillerPropertyEnd
                                            + fillerPropertyStart + "sourceName','" + rawAttr.sourceName + fillerPropertyEnd
                                            + fillerPropertyStart + "description','" + rawAttr.description + fillerPropertyEnd
                                            + ".property('entityType','attribute')";
                    string vertexKeyAttr = "AddVertex " + vertexIndex.ToString();
                    vertexIndex++;
                    gremlinQueries.Add(vertexKeyAttr, queryStringAttr);
                    Console.WriteLine(vertexKeyAttr + " , " + queryStringAttr);
                }
                //Add Dependency         
                foreach (InnerDependency depEntity in tmpEntity.dependencyList)
                {
                    string queryStringDep = startNode + depEntity.name + fillerPropertyEnd
                                            + fillerPropertyStart + "entityName','" + depEntity.name + fillerPropertyEnd
                                            + fillerPropertyStart + "dataType','" + depEntity.dataType + fillerPropertyEnd
                                            + fillerPropertyStart + "sourceName','" + depEntity.sourceName + fillerPropertyEnd
                                            + fillerPropertyStart + "description','" + depEntity.description + fillerPropertyEnd
                                            + ".property('entityType','dependency')"; ;
                    string vertexKeyDep = "AddVertex " + vertexIndex.ToString();
                    vertexIndex++;
                    gremlinQueries.Add(vertexKeyDep, queryStringDep);
                    Console.WriteLine(vertexKeyDep + " , " + queryStringDep);
                }

            }//end of adding vertices
            //add edges
            foreach (var tmpEntity in entityList)
            {
                //Add Parent - Child Dependency
                //"AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
                string queryString = "g.V('" + tmpEntity.Id + "').addE('ChildOf').to(g.V('" + tmpEntity.getParentEntityName() + "'))";                     
                string edgeKey = "AddEdge " + edgeIndex.ToString();
                edgeIndex++;
                gremlinQueries.Add(edgeKey, queryString);
                Console.WriteLine(edgeKey + " , " + queryString);
                
                //add attribute dependency
                foreach ( RawAttribute rawAttr in tmpEntity.attributeList)
                {
                    //"AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
                    string queryStringAttr = "g.V('" + tmpEntity.getEntityName() + "').addE('" + rawAttr.relationship + "').to(g.V('" + rawAttr.name + "'))";
                    string edgeKeyAttr = "AddEdge " + edgeIndex.ToString();
                    edgeIndex++;
                    gremlinQueries.Add(edgeKeyAttr, queryStringAttr);
                    Console.WriteLine(edgeKeyAttr + " , " + queryStringAttr);
                }
                
                //Add Foreign Key Dependency
                /*    public string relationship;public string displayName;public string description;*/
                foreach ( InnerDependency innerDep in tmpEntity.dependencyList)
                {
                    //"AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
                    string queryStringDep = "g.V('" + tmpEntity.getEntityName() + "').addE('"+ innerDep.relationship +"').to(g.V('" + innerDep.name + "'))";
                    string edgeKeyDep = "AddEdge " + edgeIndex.ToString();
                    edgeIndex++;
                    gremlinQueries.Add(edgeKeyDep, queryStringDep);
                    Console.WriteLine(edgeKeyDep + " , " + queryStringDep);
                }
            }
        }

        public static List<InnerDependency> ExtractDependency(RawAttribute result)
        {
            List<InnerDependency> listInnerDep = new List<InnerDependency>();
            //is the relationship attribute in RawAttribute a Expando
            var relType = result.relationship.GetType();
            if (relType.Name == "String")
            {
                //do nothing
            } else
            {
                //extract dependency
                foreach (KeyValuePair<string, object> kvp in ((IDictionary<string, object>)result.relationship))
                {
                    InnerDependency objInnerArgs = new InnerDependency();
                    if (kvp.Key is "appliedTraits")
                    {
                        List<object> objSomething = (List<object>)kvp.Value;
                        foreach (var kvp5 in objSomething)
                        {
                            foreach (KeyValuePair<string, object> kvp6 in ((IDictionary<string, object>)kvp5))
                            {
                                if (kvp6.Key is "arguments")
                                {
                                    //get a list of objects
                                    List<object> objSomething2 = (List<object>)kvp6.Value;
                                    foreach (var kvp7 in objSomething2)
                                    {
                                        if (kvp7.GetType().Name == "String")
                                            continue;
                                        foreach (KeyValuePair<string, object> kvp8 in ((IDictionary<string, object>)kvp7))
                                        {
                                            if (kvp8.Key is "name")
                                            {
                                                objInnerArgs.relationship = kvp8.Value.ToString();
                                            }
                                            else if (kvp8.Key is "value")
                                            {
                                                InnerDependency tmpobjID = Mapper.Map<InnerDependency>(kvp8.Value);
                                                if(!(objInnerArgs.relationship is null))
                                                    tmpobjID.relationship = objInnerArgs.relationship;
                                                if((tmpobjID.relationship == "ChildOf") || (tmpobjID.relationship == "foreignKeyAttribute"))
                                                    listInnerDep.Add(tmpobjID);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return listInnerDep;
        }
    }

    public class Entity
    {
        public string Id;
        private string entityName;
        private string parent;
        public List<RawAttribute> attributeList;
        public List<InnerDependency> dependencyList;

        public Entity()
        {
            entityName = String.Empty;
            Id = String.Empty;
            parent = String.Empty;
            this.attributeList = new List<RawAttribute>();
            this.dependencyList = new List<InnerDependency>();
        }
        public Entity(string name, string parent, string id)
        {
            entityName = name;
            this.Id = id;
            this.parent = parent;
            this.attributeList = new List<RawAttribute>();
            this.dependencyList = new List<InnerDependency>();
        }

        public void setEntityId(string name)
        {
            Id = name;
        }

        public void setParentEntityName(string name)
        {
            parent = name;
        }
        public string getParentEntityName()
        {
            return parent;
        }
        public void setEntityName(string name)
        {
            entityName = name;
        }
        public string getEntityName()
        {
            return entityName;
        }
    };

    public class RawAttribute
    {
        public dynamic relationship;
        public string dataType;
        public string name;
        public string sourceName;
        public string displayName;
        public string description;

    };
    /*
    public class Dependency
    {
        public string displayName;
        //public string relationship;
        public string relationshipReference;
        public bool isNullable;
    };

    public class ComplexRelationship
    {
        string relationshipReference;
        List<AppliedTraits> appliedTraits;

        public ComplexRelationship()
        {
            this.appliedTraits = new List<AppliedTraits>();// appliedTraits;
        }
    };

    public class AppliedTraits
    {
        string traitReference;
        List<Arguments> arguments;
        public AppliedTraits()
        {
            this.arguments = new List<Arguments>();
        }
    };

    public class Arguments
    {
        string name;
        InnerDependency value;

        public Arguments()
        {
            this.value = new InnerDependency();
        }
    };
    */
    public class InnerDependency
    {
        public string relationship;
        public string dataType;
        public string name;
        public string displayName;
        public bool isNullable;
        public string sourceName;
        public string description;
    };
}
