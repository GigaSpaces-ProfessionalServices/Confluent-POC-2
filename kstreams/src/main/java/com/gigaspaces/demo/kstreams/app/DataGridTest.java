package com.gigaspaces.demo.kstreams.app;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.client.ReadByIdsResult;
import com.gigaspaces.demo.kstreams.model.Organization;
import com.gigaspaces.demo.kstreams.model.Person;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.space.CannotFindSpaceException;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.util.*;
import java.util.stream.Collectors;

public class DataGridTest {
    private static final String SPACE = "demo";
    public static void main(String[] args) {
        GigaSpace gigaSpace = getOrCreateSpace(args.length == 0 ? null : args[0]);
        System.out.println("Connected to space " + gigaSpace.getName());
        System.out.println();
        System.out.println(">>> Cache query example started.");
        // Populate caches.
        initialize(gigaSpace);

        // Example for SCAN-based query based on a predicate.
        scanQuery(gigaSpace);

        // Example for TEXT-based querying for a given string in peoples resumes.
        textQuery(gigaSpace);

        print("Cache query example finished.");

    }


    private static Map<String, Object>[] embbededscanForPersonWithOrg(GigaSpace embeddedClient,
                                                                               SQLQuery<Person> query) {
        Person[] all = embeddedClient.readMultiple(query);
        if(null == all)
            return null;
        Long[] orgIds = Arrays.stream(all).map(p-> p.getOrgId()).toArray(Long[]::new);
        ReadByIdsResult<Organization> results = embeddedClient.readByIds(new IdsQuery<Organization>(Organization.class, orgIds));
        Iterator<Organization> it = results.iterator();
        Map<Long, Organization> orgs = new HashMap<>();
        while(it.hasNext()) {
            Organization o = it.next();
            orgs.put(o.getId(), o);
        }

        Map<String,Object>[] ret_list = new Map[all.length];

        for(int index = 0; index < all.length; index++) {
            Person p = all[index];
            Organization o = orgs.get(p.getOrgId());
            Map<String, Object> record = new HashMap<>();
            record.put("PersonId",  p.getId());
            record.put("LastName",  p.getLastName());
            record.put("FirstName",  p.getFirstName());
            record.put("Salary",  p.getSalary());
            record.put("OrgId",  o.getId());
            record.put("OrgName",  o.getName());
            record.put("OrgType",  o.getType().name());
            ret_list[index] = record;
        }
        return ret_list;
    }

    /**
     * Example for scan query based on a predicate using binary objects.
     */
    private static void scanQuery(GigaSpace client) {
        try {
            DistributedTask<Map<String, Object>[],List<Map<String, Object>>> task =
                    new DistributedTask<Map<String, Object>[], List<Map<String, Object>>>() {
                @Override
                public List<Map<String, Object>> reduce(List<AsyncResult<Map<String, Object>[]>> results) throws Exception {
                    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
                    for (AsyncResult<Map<String, Object>[]> result : results) {
                        if (result.getException() != null) {
                            throw result.getException();
                        }
                        Collections.addAll(list, result.getResult());
                    }
                    return list;
                }

                @TaskGigaSpace
                transient GigaSpace embedGiga;

                @Override
                public Map<String, Object>[] execute() throws Exception {
                    Map<String, Object>[] maps = DataGridTest.embbededscanForPersonWithOrg(embedGiga,
                            new SQLQuery<Person>(Person.class, "salary <= 1000"));
                    return maps;
                }
            };

            AsyncFuture<List<Map<String, Object>>> res =  client.execute(task);
            List<Map<String, Object>> results = res.get();
            print(results);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private static void textQuery(GigaSpace client) {

        //  Query for all people with "Master Degree" in their resumes.
        SQLQuery<Person> mastersQuery = new SQLQuery<Person>(Person.class, "resume text:match ?");
        mastersQuery.setParameter(1, "Master");


        // Query for all people with "Bachelor Degree" in their resumes.
        SQLQuery<Person> bachelorsQuary = new SQLQuery<Person>(Person.class, "resume text:match ?");
        bachelorsQuary.setParameter(1, "Bachelor");
        Person[] masters = client.readMultiple(mastersQuery);
        print("Following people have 'Master Degree' in their resumes: ",
                null != masters? Arrays.asList(masters): Arrays.asList(new Person[0]));
        print("Following people have 'Bachelor Degree' in their resumes: ", client.iterator(bachelorsQuary));
    }


    /**
     * Populate cache with test data.
     */
    private static void initialize(GigaSpace client) {

        client.clear(null);

        // Organizations.
        Organization org1 = new Organization("Gigaspaces");
        Organization org2 = new Organization("Other");
        client.write(org1);
        client.write(org2);

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John2", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane2", "Smith", 2000, "Jane Smith has Master Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        client.write(p1);
        client.write(p2);
        client.write(p3);
        client.write(p4);
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            if(next instanceof Map) {
                Map<String, Object> map = (Map<String, Object>)next;
                System.out.println(">>>    " + map.keySet().stream()
                        .map(key -> key + "=" + map.get(key))
                        .collect(Collectors.joining(", ", "{", "}")));
            }
            else
                System.out.println(">>>     " + next);
    }
    public static GigaSpace getOrCreateSpace(String spaceName) {
        if (spaceName == null) {
            System.out.println("Space name not provided - creating an embedded space...");
            return new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("mySpace")).create();
        } else {
            System.out.printf("Connecting to space %s...%n", spaceName);
            try {
                return new GigaSpaceConfigurer(new SpaceProxyConfigurer(spaceName)).create();
            } catch (CannotFindSpaceException e) {
                System.err.println("Failed to find space: " + e.getMessage());
                throw e;
            }
        }
    }
}
