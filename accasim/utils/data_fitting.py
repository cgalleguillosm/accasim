"""
MIT License

Copyright (c) 2017 cgalleguillosm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import warnings
from multiprocessing import Pool, cpu_count
from scipy import stats as _statistical_distributions
from numpy import sum, power, roll, histogram


class DistributionFitting:
    KINT64MAX = 2 ** 63 - 1
    CONTINUOUS_DISTRIBUTIONS = {
        'alpha': 'An alpha continuous random variable.',
        'anglit': 'An anglit continuous random variable.',
        'arcsine': 'An arcsine continuous random variable.',
        'argus': 'Argus distribution',
        'beta': 'A beta continuous random variable.',
        'betaprime': 'A beta prime continuous random variable.',
        'bradford': 'A Bradford continuous random variable.',
        'burr': 'A Burr (Type III) continuous random variable.',
        'burr12': 'A Burr (Type XII) continuous random variable.',
        'cauchy': 'A Cauchy continuous random variable.',
        'chi': 'A chi continuous random variable.',
        'chi2': 'A chi-squared continuous random variable.',
        'cosine': 'A cosine continuous random variable.',
        'crystalball': 'Crystalball distribution',
        'dgamma': 'A double gamma continuous random variable.',
        'dweibull': 'A double Weibull continuous random variable.',
        'erlang': 'An Erlang continuous random variable.',
        'expon': 'An exponential continuous random variable.',
        'exponnorm': 'An exponentially modified Normal continuous random variable.',
        'exponweib': 'An exponentiated Weibull continuous random variable.',
        'exponpow': 'An exponential power continuous random variable.',
        'f': 'An F continuous random variable.',
        'fatiguelife': 'A fatigue-life (Birnbaum-Saunders) continuous random variable.',
        'fisk': 'A Fisk continuous random variable.',
        'foldcauchy': 'A folded Cauchy continuous random variable.',
        'foldnorm': 'A folded normal continuous random variable.',
        'frechet_r': 'A frechet_r continuous random variable.',
        'frechet_l': 'A frechet_l continuous random variable.',
        'genlogistic': 'A generalized logistic continuous random variable.',
        'gennorm': 'A generalized normal continuous random variable.',
        'genpareto': 'A generalized Pareto continuous random variable.',
        'genexpon': 'A generalized exponential continuous random variable.',
        'genextreme': 'A generalized extreme value continuous random variable.',
        'gausshyper': 'A Gauss hypergeometric continuous random variable.',
        'gamma': 'A gamma continuous random variable.',
        'gengamma': 'A generalized gamma continuous random variable.',
        'genhalflogistic': 'A generalized half-logistic continuous random variable.',
        'gilbrat': 'A Gilbrat continuous random variable.',
        'gompertz': 'A Gompertz (or truncated Gumbel) continuous random variable.',
        'gumbel_r': 'A right-skewed Gumbel continuous random variable.',
        'gumbel_l': 'A left-skewed Gumbel continuous random variable.',
        'halfcauchy': 'A Half-Cauchy continuous random variable.',
        'halflogistic': 'A half-logistic continuous random variable.',
        'halfnorm': 'A half-normal continuous random variable.',
        'halfgennorm': 'The upper half of a generalized normal continuous random variable.',
        'hypsecant': 'A hyperbolic secant continuous random variable.',
        'invgamma': 'An inverted gamma continuous random variable.',
        'invgauss': 'An inverse Gaussian continuous random variable.',
        'invweibull': 'An inverted Weibull continuous random variable.',
        'johnsonsb': 'A Johnson SB continuous random variable.',
        'johnsonsu': 'A Johnson SU continuous random variable.',
        'kappa4': 'Kappa 4 parameter distribution.',
        'kappa3': 'Kappa 3 parameter distribution.',
        'ksone': 'General Kolmogorov-Smirnov one-sided test.',
        'kstwobign': 'Kolmogorov-Smirnov two-sided test for large N.',
        'laplace': 'A Laplace continuous random variable.',
        'levy': 'A Levy continuous random variable.',
        'levy_l': 'A left-skewed Levy continuous random variable.',
        'levy_stable': 'A Levy-stable continuous random variable.',
        'logistic': 'A logistic (or Sech-squared) continuous random variable.',
        'loggamma': 'A log gamma continuous random variable.',
        'loglaplace': 'A log-Laplace continuous random variable.',
        'lognorm': 'A lognormal continuous random variable.',
        'lomax': 'A Lomax (Pareto of the second kind) continuous random variable.',
        'maxwell': 'A Maxwell continuous random variable.',
        'mielke': 'A Mielke\'s Beta-Kappa continuous random variable.',
        'nakagami': 'A Nakagami continuous random variable.',
        'ncx2': 'A non-central chi-squared continuous random variable.',
        'ncf': 'A non-central F distribution continuous random variable.',
        'nct': 'A non-central Student\'s T continuous random variable.',
        'norm': 'A normal continuous random variable.',
        'pareto': 'A Pareto continuous random variable.',
        'pearson3': 'A pearson type III continuous random variable.',
        'powerlaw': 'A power-function continuous random variable.',
        'powerlognorm': 'A power log-normal continuous random variable.',
        'powernorm': 'A power normal continuous random variable.',
        'rdist': 'An R-distributed continuous random variable.',
        'reciprocal': 'A reciprocal continuous random variable.',
        'rayleigh': 'A Rayleigh continuous random variable.',
        'rice': 'A Rice continuous random variable.',
        'recipinvgauss': 'A reciprocal inverse Gaussian continuous random variable.',
        'semicircular': 'A semicircular continuous random variable.',
        'skewnorm': 'A skew-normal random variable.',
        't': 'A Student\'s T continuous random variable.',
        'trapz': 'A trapezoidal continuous random variable.',
        'triang': 'A triangular continuous random variable.',
        'truncexpon': 'A truncated exponential continuous random variable.',
        'truncnorm': 'A truncated normal continuous random variable.',
        'tukeylambda': 'A Tukey-Lamdba continuous random variable.',
        'uniform': 'A uniform continuous random variable.',
        'vonmises': 'A Von Mises continuous random variable.',
        'vonmises_line': 'A Von Mises continuous random variable.',
        'wald': 'A Wald continuous random variable.',
        'weibull_min': 'Weibull minimum continuous random variable.',
        'weibull_max': 'Weibull maximum continuous random variable.',
        'wrapcauchy': 'A wrapped Cauchy continuous random variable.'
    }

    def __init__(self, dist_names=[], blacklist=[], cpus=None, default='gamma'):
        """

        :param dist_names:
        :param blacklist:
        :param cpus:
        :param default:
        """
        assert (isinstance(dist_names, list))
        assert (isinstance(blacklist, list))
        self.dist_names = [dist_name for dist_name, description in self.CONTINUOUS_DISTRIBUTIONS.items() if
                           not (dist_names in blacklist)] if not dist_names else dist_names
        self.default_distribution = default
        self._set_cpus(cpus)

    def auto_best_fit(self, data):
        """

        :param y:
        :return:
        """
        y, x = histogram(data, bins='auto', density=True)
        self.data = data
        self.y = y
        self.x = (x + roll(x, -1))[:-1] / 2.0
        try:
            results = []
            with Pool(self.cpus) as p:
                for dist_name in self.dist_names:
                    p.apply_async(self._best_fit, (dist_name,), callback=results.append)
                p.close()
                p.join()
            return (sorted(results, key=lambda r: r[1])[0], self.x, self.y,)
        except IndexError:
            return (self._best_fit(self.default_distribution), self.x, self.y,)

    def _set_cpus(self, cpus):
        """

        :param cpus:
        :return:
        """
        if cpus:
            self.cpus = cpus

        try:
            self.cpus = cpu_count() - 1
        except NotImplementedError:
            self.cpus = 1

    def _best_fit(self, dist_name):
        """

        :param dist_name:
        :return:
        """
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dist = getattr(_statistical_distributions, dist_name)
            params = dist.fit(self.data)
            arg = params[:-2]
            loc = params[-2]
            scale = params[-1]
            pdf = dist.pdf(self.x, loc=loc, scale=scale, *arg)
            sse = sum(power(self.y - pdf, 2.0))
        return (dist_name, sse, params)
