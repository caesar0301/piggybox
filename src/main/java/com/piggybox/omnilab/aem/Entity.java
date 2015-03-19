package com.piggybox.omnilab.aem;

import com.piggybox.utils.tree.TreeNode;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * A POJO class to represent entity in AEM.
 * @author chenxm
 */
class Entity {
    public Double start;	// the start time
    public Double end;	// the end time
    public String url;
    public String referrer;
    public String type;
    public Integer aemLastType = null; // classified type against the last entity.
    public Integer aemPredType = null; // classified type against its preceding entity.
    public boolean isDummy = false; // If this entity is dummy.
    //E.g. if a referrer entity is lost but some entities refer to it,
    //we create a dummy referrer entity to lead followers.
    public boolean hasFakeReferrer = false; // It indicates that this entity is linked to its preceding entity without referrer.

    private static final double DUR_LOW_BOUND = 0.1; //# sec, a lower-bound duration
    private String ID = null;
    private Tuple originalTuple = null;
    private TreeNode node = null; // the node this entity linked to.

    /**
     * Initialize an entity with a tuple.
     * @param tuple
     * @throws org.apache.pig.backend.executionengine.ExecException
     */
    public Entity(Tuple tuple) throws ExecException {
        this((Double)tuple.get(0),
                (Double)tuple.get(1),
                (String)tuple.get(2),
                (String)tuple.get(3),
                (String)tuple.get(4),
                (String)tuple.get(5),
                tuple);
    }

    /**
     * Private initialization of an entity with six-fields.
     * @param start
     * @param end
     * @param url
     * @param referrer
     * @param type
     * @param id
     */
    public Entity(Double start, Double end, String url, String referrer, String type, String id, Tuple original){
        this.start = start;
        if ( end == null )
            this.end = start + DUR_LOW_BOUND; // simply calculation to add a minimum duration.
        else
            this.end = end;
        this.url = url;
        this.referrer = referrer;
        this.type = type;
        this.ID = id;
        this.isDummy = false;
        this.hasFakeReferrer = false;
        this.node = new TreeNode(this);
        this.originalTuple = original;
        if ( original == null )
            this.isDummy = true;
    }

    public Tuple getTuple(){
        return this.originalTuple;
    }

    /**
     * Get and set TreeNode of this entity.
     */
    public TreeNode getTreeNode(){
        return this.node;
    }
    public void setTreeNode(TreeNode treeNode){
        this.node = treeNode;
    }

    /**
     * Get the child number of this entity in underlying tree structure.
     * @return
     */
    public int getChildNum(){
        return this.node.getChildCount();
    }

    /**
     * Get the overlap time duration of two entities;
     * @param e
     * @return overlapped duration: <0, =0, or >0
     */
    public double overlap(Entity e){
        double s1 = this.start;
        double e1 = this.end;
        double s2 = e.start;
        double e2 = e.end;
        if (s2 >= s1)
            return Math.min(e1, e2) - s2;
        else
            return Math.min(e1, e2) - s1;
    }

    /**
     * Get time difference of starting times of two entities.
     * @param e
     * @return Time difference
     */
    public double headDiff(Entity e){
        double s1 = this.start;
        double s2 = e.start;
        return Math.abs(s1-s2);
    }

    /**
     * Get time difference of ending times of two entities.
     * @param e
     * @return
     */
    public double tailDiff(Entity e){
        double e1 = this.end;
        double e2 = e.end;
        return Math.abs(e1-e2);
    }

    /**
     * Check if the entity is a base element of web page, e.g. a HTML file
     * which describes the textual content and framework of a web page.
     * @param e
     */
    public boolean isWebPageBase(){
        if ( this.type != null && this.type.contains("text"))
            return true;
        return false;
    }

    /**
     * Get the duration of this entity.
     * @return Elapsed time from start to end.
     */
    public double duration(){
        return this.end - this.start;
    }

    /**
     * Get unique ID of this entity.
     * @return
     */
    public String getID(){
        return this.ID.toString();
    }

    /**
     * Check if this entity is the preceding of given e;
     * @param e
     * @return
     */
    public boolean isPredEntity(Entity e){
        if ( this.url != null && e.referrer != null && this.url.equals(e.referrer))
            return true;
        return false;
    }
}
