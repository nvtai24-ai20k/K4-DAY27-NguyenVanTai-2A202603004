# Reflection (≤1 page)

**Results:** practice 49.66 (TPR 1.00, FPR 0.011) · public 48.18 (TPR 1.00, FPR 0.00,
9% over budget) · **private 32.92 (TPR 0.67, FPR 0.014, no overage)**.

**Approach in one paragraph.** One metered call per event, no more and no fewer. The
tools are deterministic, so a second call tells you nothing new. Skipping events is also
a bad deal: going one credit over budget costs 20/budget points, while an event's expected
catch is worth roughly P(fault)·50/n_faulty points, which is several times more. The
score formula weighs a miss about 4–5× more than a false alarm (0.5/n_faulty vs
0.3/n_clean), so I set thresholds at 2–2.5σ instead of the published 3σ. The two-sided
baselines give mean and σ directly. For the one-sided ones (only `max` = mean+3σ is
published), I estimate the clean mean online as the running median and take
σ = (max − median)/3. Structural facts are learned from the stream as majority behaviour:
the expected upstream set (the payload declares only `raw.orders`, but clean runs read
`raw.orders` + `raw.customers`), the downstream count, and the payload columns. Contracts
alert on any `violations` and on freshness above the producer's own declared SLA.

**Which fault types were hardest to catch, and why?**
- **The subtle-magnitude continuous faults** (volume, staleness, distribution shift,
  feature skew, embedding drift / corpus age). On private I caught only two-thirds of the
  faults. The structural faults (schema/type violations, missing upstream, orphan outputs)
  are binary and essentially free to catch. So the third I missed are almost certainly
  continuous-metric faults injected *inside* my 2–2.5σ bands. No single-metric threshold
  can separate those from clean variance without also firing on clean events.
- **Feature skew needed a calibration fix.** On public, clean batches reached
  |mean_shift_sigma| ≈ 0.45–0.49, which is above the published 0.41 "3σ" max. |shift| is
  half-normal, so a mean+3σ bound computed on the absolute value sits too low in the tail.
  I raised that threshold to 1.5× the published max to remove two false alarms. In
  hindsight this probably cost private catches: subtle skew there is likely smaller than
  the ≥2.3σ examples practice showed.
- **Early-stream faults.** Before the running median warms up, only the 3σ baseline
  applies. Shortening warm-up from 5 to 3 observations caught one subtle fault on public.

**What would you change about your cost/coverage tradeoff, if you had another pass?**
- **Coverage was right; sensitivity was wrong.** Private had cost headroom and a low FPR
  (0.014). At these weights, every extra false alarm costs ~0.2 points and every extra
  catch earns ~1 point, so I should have accepted an FPR of 4–6% in exchange for more
  TPR. Concretely: per-metric thresholds nearer 1.5–1.75σ, and feature skew back at or
  below the published max.
- **Score in likelihood space, not with per-field cut-offs.** Turn each metric into a
  z-score, combine the z-scores per event (e.g. the largest z with a lower bar when two
  fields are elevated at once), and alert when the posterior P(fault) passes the
  break-even of about 0.18 implied by the scoring weights.
- **Model clean variance from the stream more carefully.** Estimate the spread of the
  signed feature shift and of centroid drift online (median/MAD on clean-looking
  history), instead of trusting a 3σ bound that assumes Gaussian data.
- **Validate on held-out data, not on public bands.** I tuned against public's exact
  TPR/FPR and got it to 1.00/0.00. That overfit a stream whose faults were mostly
  obvious, and the result did not transfer to private's subtle mix. A synthetic
  fault-injection harness, sweeping magnitude from 1σ to 3σ, would have shown the
  sensitivity gap before the one private attempt.
