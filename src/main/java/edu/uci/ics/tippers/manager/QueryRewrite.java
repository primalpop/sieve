package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.model.data.Infrastructure;
import edu.uci.ics.tippers.model.data.User;
import edu.uci.ics.tippers.model.query.BasicQuery;

/**
 * Created by cygnus on 7/7/17.
 */
public class QueryRewrite {


//    public static String rewriteQuery(int queryIndex, int userPreds, int locPreds, boolean optimize) {
//        String rewrittenQuery = BasicQuery.queryList[queryIndex];
//        userPreds = (userPreds > User.users.length) ? User.users.length : userPreds;
//        locPreds = (locPreds > Infrastructure.locations.length) ? Infrastructure.locations.length : locPreds;
//
//        if (userPreds == 0) {
//            String predicate = "location = \'%d\'";
//            for (int i = 0; i < locPreds; i++) {
//                if (i == 0) {
//                    rewrittenQuery += String.format(predicate, Infrastructure.locations[i]);
//                } else {
//                    rewrittenQuery += " OR ";
//                    rewrittenQuery += String.format(predicate, Infrastructure.locations[i]);
//                }
//            }
//        }
//
//        else if(locPreds == 0) {
//            String predicate = "user_id = \'%d\'";
//            for (int i = 0; i < locPreds; i++) {
//                if (i == 0) {
//                    rewrittenQuery += String.format(predicate, User.users[i]);
//                } else {
//                    rewrittenQuery += " OR ";
//                    rewrittenQuery += String.format(predicate, User.users[i]);
//                }
//            }
//        }
//
//        else if (userPreds != 0 && locPreds !=0){
//            String predicate = "(location = \'%d\' and user_id = \'%d\')";
//            if (optimize)
//                rewrittenQuery = optimizedRewriteQuery(queryIndex, userPreds, locPreds);
//            for (int i = 0; i < userPreds; i++) {
//                for (int j =0; j < locPreds; j++) {
//                    if( i == 0 && j == 0) {
//                        rewrittenQuery += String.format(predicate, Infrastructure.locations[j], User.users[i]);
//                    }
//                    else{
//                        rewrittenQuery += " OR ";
//                        rewrittenQuery += String.format(predicate, Infrastructure.locations[j], User.users[i]);
//                    }
//                }
//            }
//        }
//        else {
//            System.out.println("No user and location predicates!!!");
//        }
//        rewrittenQuery += ";";
//        return rewrittenQuery;
//    }
//
//    public static String optimizedRewriteQuery(int queryIndex, int userPreds, int locPreds){
//        String rewrittenQuery = BasicQuery.queryList[queryIndex];
//        String user_predicate = "user_id in (";
//        String location_predicate = ") and location in (";
//        for(int i = 0; i < userPreds; i++){
//            if(i==0)
//                user_predicate += "\'" + User.users[i] + "\'";
//            else
//                user_predicate += "," + "\'" + User.users[i] + "\'";
//        }
//
//        for(int i = 0; i < locPreds; i++){
//            if(i==0)
//                location_predicate +=  "\'"  + Infrastructure.locations[i]  + "\'";
//            else
//                location_predicate += "," + "\'" + Infrastructure.locations[i] + "\'" ;
//        }
//
//        rewrittenQuery += user_predicate + location_predicate + ") and ";
//        return rewrittenQuery;
//    }
}
