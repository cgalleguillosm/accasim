/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based;

import java.util.Date;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.GridletInfo;
import xklusac.environment.Scheduler;

/**
 * Class Rejector<p>
 * Simple scheduler which rejects all submitted jobs
 * @author       Cristian Galleguillos
 */
public class Rejector implements SchedulingPolicy {

    private Scheduler scheduler;
    private int rejected;

    public Rejector(Scheduler scheduler) {
        this.scheduler = scheduler;
        this.rejected = 0;
    }

    @Override
    public void addNewJob(GridletInfo gi) {
        double runtime1 = new Date().getTime();
        Scheduler.queue.addLast(gi);
        Scheduler.runtime += (new Date().getTime() - runtime1);
    }

    @Override
    public int selectJob() {
        for (int i = 0; i < Scheduler.queue.size(); i++) {
            GridletInfo gi = (GridletInfo) Scheduler.queue.remove(i);
            try {
                gi.getGridlet().setGridletStatus(Gridlet.CANCELED);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, GridSimTags.GRIDLET_RETURN, gi.getGridlet());
            this.rejected += 1;
        }
        return 0;
    }
}
