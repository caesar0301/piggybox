package com.piggybox.omnilab.aem;

import com.google.common.net.InternetDomainName;
import org.apache.commons.logging.Log;
import org.apache.pig.backend.executionengine.ExecException;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Activity-Entity Model of mobile traffic.
 * @author chenxm
 *
 * Implementation philosophy (Activity and Entity):
 * I utilize the tree data structure contributed by Yifan Peng (github @yfpeng) as
 * the underlying structure of activities and entities.
 * In general, an activity may contains multiple entities while only one activity
 * does an entity link to.
 * You can imagine this as a two-layer graph:
 * all entities are planted on such an upper layer individually that they do not know each other;
 * on the lower layer, one TreeNode object is created for an entity (with bidirectional pointers) and linked each other (tree structure).
 * An activity only remember the root TreeNode and its entities are handled by the tree structure.
 * So you should make sure the tree has given node before performing processing.
 */
class AEM{
    // Classified entity types
    public static final int TYPE_UNCL = 9;	// unclassified
    public static final int TYPE_CONJ = 0; // conjunction
    public static final int TYPE_PRLL = 1; // parallel
    public static final int TYPE_RLYD = 2; // relayed
    public static final int TYPE_SRAL = 3; // serial
    // AEM configurations
    private static final double CONJ_ST_DIFF = 0.5; //# sec, |ts1-ts2| < 0.5
    private static final double CONF_ET_PCRT = 0.1; //# 10%, |te1-te2|/min(t1,t2) < 0.1
    private static final double RLYD_TDIFF1 = 0;	//# ts2-te1 >= 0
    private static final double RLYD_TDIFF2 = 0.5;	//# sec, ts2-te1 < 0.1
    private static final double SRAL_TDIFF1 = 0;	//# sec, ts2-te1 > 0
    private static final double SRAL_TDIFF2 = 8;	//# sec, ts2-te1 <= 10
    private static final double PRLL_OL = 0;		//# sec, overlap
    private static final double PAGE_FAT = 5;		//# Number of embedded entities of fat page
    private static final double PAGE_SLIM = 2;		//# Number of embedded entities of slim page
    private static final double READING_TIME_DEFAULT = 2;	//# sec, user reading time
    private static double READING_TIME;

    private static List<Activity> activities = new LinkedList<Activity>();
    private static Entity lastEntity = null;
    private Log logger = null;

    public AEM(){
        this(READING_TIME_DEFAULT, null);
    }

    public AEM(double readingTime, Log logger){
        AEM.READING_TIME = readingTime;
        this.logger = logger;
    }

    public int size(){
        return AEM.activities.size();
    }

    /**
     * Add a given entity to AEM model correctly.
     * @param newEntity
     * @throws org.apache.pig.backend.executionengine.ExecException
     * @throws Exception
     */
    public boolean addEntityToModel(Entity newEntity) throws ExecException {
        boolean createNew = false;
        boolean linked2activity = false;
        boolean dumpModelToBag = false; // indicate if the intermediate data are available to dump.

        if ( lastEntity == null ) createNew = true;
        else{
            newEntity.aemLastType = classify(lastEntity, newEntity);
            if (newEntity.aemLastType == AEM.TYPE_UNCL){ // 10s is involved to separate different activities forcedly.
                createNew = true;
                dumpModelToBag = true;
            }else{
                if ( newEntity.referrer != null ){ // with referrer
                    Activity act = null;
                    Entity removedEntity = null;
                    for ( int i = activities.size()-1; i >= 0; i--){ // reversed order
                        act = activities.get(i);
                        Entity refEntity = act.findReferrerEntity(newEntity);
                        if ( refEntity != null ){ // referrer found
                            newEntity.aemLastType = classify(lastEntity, newEntity);
                            newEntity.aemPredType = classify(refEntity, newEntity);
                            act.addEntity(newEntity, refEntity);
                            linked2activity = true;
                            // Check if cut the activity
                            // TODO: for requests generated by redirection, the original URL should also be included
                            // in current activity.
                            int pch = refEntity.getChildNum(); // child number
                            boolean isPageBase = refEntity.isWebPageBase(); // if it is a web page base
                            boolean isRefRoot = (act.hasRoot(refEntity) || refEntity.hasFakeReferrer);
                            if (!isRefRoot && isPageBase && pch > PAGE_FAT ){
                                removedEntity = refEntity;
                            }
                            if (!isRefRoot && newEntity.aemLastType==TYPE_SRAL && isPageBase && pch>PAGE_SLIM){ // removed bug
                                removedEntity = refEntity;
                            }
                            if (!isRefRoot && newEntity.aemPredType==TYPE_UNCL && isPageBase && pch>PAGE_SLIM){
                                removedEntity = refEntity;
                            }
                            break;
                        }
                    }
                    // Remove cutEntities as new activities
                    if ( removedEntity != null ) {
                        for ( int j = activities.size()-1; j >= 0; j--){ // reversed order
                            Activity a = activities.get(j);
                            if ( a.contains(removedEntity)){
                                a.removeEntity(removedEntity);
                                Activity newA = new Activity(removedEntity);
                                activities.add(newA);
                                logger.warn("New activity. Model size: " + size());
                                break;
                            }
                        }
                    }
                } else { //without referrer
                    String sdm1 = getTopPrivateDomain(lastEntity.url);
                    String sdm2 = getTopPrivateDomain(newEntity.url);
                    if ( newEntity.aemLastType != AEM.TYPE_SRAL || (Math.abs(lastEntity.overlap(newEntity)) < AEM.READING_TIME &&
                            sdm1 != null && sdm2!=null && sdm1.equals(sdm2))) {
                        // create a fake link to the preceding
                        for ( int i = activities.size()-1; i >= 0; i--){ // reversed order
                            Activity act = activities.get(i);
                            if ( act.contains(lastEntity)){
                                // add e to this activity
                                newEntity.aemPredType = newEntity.aemLastType;
                                newEntity.hasFakeReferrer = true;
                                linked2activity = true;
                                act.addEntity(newEntity, lastEntity);
                                break;
                            }
                        }
                    } else {
                        createNew = true;
                    }
                }
            }
        }
        if ( createNew || !linked2activity ){
            Activity newActivity = new Activity();
            String ref = newEntity.referrer;
            if ( ref != null ){
                Entity dummyEntity = new Entity(newEntity.start, null, newEntity.referrer, null, null, null, null);
                dummyEntity.isDummy = true; // make sure isDummy set
                newActivity.addEntity(dummyEntity, null);
                newActivity.addEntity(newEntity, dummyEntity);
            } else
                newActivity.addEntity(newEntity, null);
            AEM.activities.add(newActivity);
        }
        lastEntity = newEntity;
        return dumpModelToBag;
    }

    /**
     * Get activities from startIndex to endIndex at appended order.
     * @return
     */
    public List<Activity> getActivities(int startIndex, int endIndex){
        return AEM.activities.subList(startIndex, endIndex);
    }

    /**
     * Remove activities from startIndex to endIndex at appended order.
     */
    public void removeActivities(int startIndex, int endIndex){
        AEM.activities.subList(startIndex, endIndex).clear();
    }

    /**
     * Classify the relationship between entities as one of declairing types in AEM.
     * @param e1
     * @param e2
     * @return Classified type.
     */
    public int classify(Entity e1, Entity e2){
        int type = TYPE_UNCL;
        if ( this.isConjunction(e1, e2) )
            type = TYPE_CONJ;
        else if ( this.isParallel(e1, e2) )
            type = TYPE_PRLL;
        else if ( this.isRelayed(e1, e2) )
            type = TYPE_RLYD;
        else if ( this.isSerial(e1, e2) )
            type = TYPE_SRAL;
        return type;
    }

    /**
     * Check if two entities are classified as relayed relationship.
     * @param e1
     * @param e2
     * @return
     */
    private boolean isRelayed(Entity e1, Entity e2){
        double ol = e1.overlap(e1);
        if ( ol <= 0 && Math.abs(ol) >= AEM.RLYD_TDIFF1 && Math.abs(ol) < AEM.RLYD_TDIFF2 )
            return true;
        return false;
    }

    /**
     * Check if two entities are classified as serial relationship.
     * @param e1
     * @param e2
     * @return
     */
    private boolean isSerial(Entity e1, Entity e2){
        double ol = e1.overlap(e2);
        if ( ol <= 0 && Math.abs(ol) >= AEM.SRAL_TDIFF1 && Math.abs(ol) <= AEM.SRAL_TDIFF2)
            return true;
        return false;
    }

    /**
     * Check if two entities are classified as conjunction relationship.
     * @param e1
     * @param e2
     * @return
     */
    private boolean isConjunction(Entity e1, Entity e2){
        double hd = e1.headDiff(e2);
        double td = e1.tailDiff(e2);
        double d1 = e1.duration();
        double d2 = e2.duration();
        // Get top private domains
        String sdm1 = getTopPrivateDomain(e1.url);
        String sdm2 = getTopPrivateDomain(e2.url);
        if ( hd <= AEM.CONJ_ST_DIFF && td/Math.min(d1, d2) < AEM.CONF_ET_PCRT &&
                sdm1 != null && sdm2 != null && sdm1.equals(sdm2))
            return true;
        return false;
    }

    private String getTopPrivateDomain(String url){
        String tpd = getHost(url);
        try {
            tpd = InternetDomainName.from(tpd).topPrivateDomain().toString();
        } catch (Exception e) {}
        return tpd; // maybe null
    }

    private String getHost(String url){
        if (url != null ){
            Pattern pattern = Pattern.compile("^(?:\\w+:?//)?([^:\\/\\?&]+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(url);
            if ( matcher.find() ){
                return matcher.group(1);
            }
        }
        return url;
    }

    /**
     * Check if two entities are classified as parallel relationship.
     * @param e1
     * @param e2
     * @return
     */
    private boolean isParallel(Entity e1, Entity e2){
        double ol = e1.overlap(e2);
        if ( ol > AEM.PRLL_OL )
            return true;
        return false;
    }
}