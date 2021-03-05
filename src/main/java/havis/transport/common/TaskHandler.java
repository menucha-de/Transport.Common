package havis.transport.common;

import havis.transport.FutureSendTask;
import havis.transport.Subscription;
import havis.transport.TransportException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

public class TaskHandler {

	private static class Task {
		private Subscription subscription;
		private FutureSendTask task;

		public Task(Subscription subscription, FutureSendTask task) {
			this.subscription = subscription;
			this.task = task;
		}

		public Subscription getSubscription() {
			return this.subscription;
		}

		public FutureSendTask getTask() {
			return this.task;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((subscription == null) ? 0 : subscription.hashCode());
			result = prime * result + ((task == null) ? 0 : task.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Task other = (Task) obj;
			if (subscription == null) {
				if (other.subscription != null)
					return false;
			} else if (!subscription.equals(other.subscription))
				return false;
			if (task == null) {
				if (other.task != null)
					return false;
			} else if (!task.equals(other.task))
				return false;
			return true;
		}
	}

	private CopyOnWriteArrayList<Task> tasks = new CopyOnWriteArrayList<>();
	private CopyOnWriteArrayList<Subscription> cancellations = new CopyOnWriteArrayList<>();
	private ExecutorService service = Executors.newSingleThreadExecutor();

	public TaskHandler() {
		this.service.submit(new Runnable() {
			@Override
			public void run() {
				boolean interrupted;
				try {
					while (!(interrupted = Thread.currentThread().isInterrupted())) {
						// handle cancellations first
						cancellations.removeIf(new Predicate<Subscription>() {
							@Override
							public boolean test(final Subscription subscription) {
								tasks.removeIf(new Predicate<Task>() {
									@Override
									public boolean test(Task t) {
										if (subscription != null && subscription.equals(t.getSubscription())) {
											t.getTask().cancel(true);
											return true;
										}
										return false;
									}
								});
								return true;
							}
						});

						List<Task> removeTasks = new ArrayList<Task>();
						for (Task task : tasks) {
							if (task.getTask().isDone() || task.getTask().isCancelled()) {
								removeTasks.add(task);
								try {
									task.getTask().get();
								} catch (TransportException e) {
									// ignore
								}
							}
						}

						if (removeTasks.size() > 0)
							tasks.removeAll(removeTasks);

						Thread.sleep(300);
					}
				} catch (InterruptedException e) {
					interrupted = true;
				}

				if (interrupted) {
					// cancel all tasks (using this method makes sure that the
					// list is not manipulated while we cancel all tasks)
					tasks.removeIf(new Predicate<Task>() {
						@Override
						public boolean test(Task t) {
							t.getTask().cancel(true);
							return true;
						}
					});
					cancellations.clear();
				}
			}
		});
	}

	protected FutureSendTask addTask(Subscription subscription, FutureSendTask task) {
		this.tasks.add(new Task(subscription, task));
		return task;
	}

	protected void cancelTasksFor(Subscription subscription) {
		this.cancellations.add(subscription);
	}

	protected void disposeTasks() {
		// also cancels all known tasks
		this.service.shutdownNow();
	}
}
