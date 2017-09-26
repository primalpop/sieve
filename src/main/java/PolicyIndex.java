import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import model.Infrastructure;
import model.Presence;
import model.User;
import rx.Observable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by cygnus on 7/5/17.
 *
 * Simple RTree for deny policies based on user and location predicates.
 *
 */
public class PolicyIndex {

    RTree<Integer, Point> tree;
    ArrayList<Presence> pList;

    public PolicyIndex(){
        pList = new ArrayList<Presence>();
    }


    public void populateTree(int numUsers, int numLocations){
        int id = 0;
        numUsers = (numUsers > User.users.length) ? User.users.length : numUsers;
        numLocations = (numLocations > Infrastructure.locations.length) ? Infrastructure.locations.length : numLocations;
        System.out.println("Number of Policies: " + numUsers * numLocations);
        for (int i = 0; i < numUsers; i++) {
            for (int j = 0; j < numLocations; j++) {
                this.tree = this.tree.add(id++, Geometries.point(User.users[i], Infrastructure.locations[j]));
            }
        }
    }


    public static ArrayList<Presence> readCSVtoArrayList(String fileCSV) {
        ArrayList<Presence> result = new ArrayList<Presence>();
        BufferedReader br = null;
        String line = "";
        String csvSplit = ",";
        if (fileCSV != null) {
            try {
                br = new BufferedReader(new FileReader(fileCSV));
                while ((line = br.readLine()) != null) {
                    String[] pString = line.split(csvSplit);
                    Presence p = new Presence();
                    p.setId(Integer.parseInt(pString[0]));
                    p.setLocation(Integer.parseInt(pString[1]));
                    p.setTimeStamp(pString[2] + " " + pString[3]);
                    p.setUser_id(Integer.parseInt(pString[4]));
                    result.add(p);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(br != null) {
                    try {
                        br.close();
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return result;
    }

    /**
     * Checking data tuples one by one against the R Tree
     *
     * Change this line to switch between accept or deny policies
     * if (match.toList().toBlocking().single().isEmpty())  do nothing, included in query result <-- deny policy
     * if (match.toList().toBlocking().single().isEmpty()) remove from query result <-- accept policy
     */
    public void checkPoliciesAgainstTuples() {
        int count = 0;
        for (Presence tuple : this.pList) {
            Observable<Entry<Integer, Point>> match =
                    this.tree.search(Geometries.point(tuple.getUser_id(), tuple.getLocation()));
            if (!match.toList().toBlocking().single().isEmpty()) {
//                System.out.println("Matching policy "  + match.toList().toBlocking().single());
//                System.out.println("Matching tuple " + tuple.toString());
                count += 1;
            }
        }
        System.out.println("# final tuples: " + count);
    }



    public void setup() {

        String firstArg = "csv_july5/3.csv";

        PolicyIndex t = new PolicyIndex();
        t.pList = readCSVtoArrayList(firstArg);
        System.out.println("# tuples: " + t.pList.size());


        int[] userNums = {1, 5, 5, 10, 100};
        int[] locationNums = {10, 10, 20, 100, 100};
        for (int i = 0; i < userNums.length; i++) {
            t.tree = RTree.create();
            t.populateTree(userNums[i], locationNums[i]);
            long startTime = System.currentTimeMillis();
            t.checkPoliciesAgainstTuples();
            long endTime = System.currentTimeMillis();
            System.err.println("With " + userNums[i] * locationNums[i] + " policies took " + (endTime - startTime) + " milliseconds");
        }

    }

}