"""
Memcache client interface with namespaces

If you need fine-grained cache invalidation you will need namespaces.


You can see bellow the basic usage:

Standard initialization:

	>>> c = Client(['localhost:11211'], debug=0)
	>>> c.flush_all()

Create two variable in separate namespaces:

	>>> c.ns_set('ns1', 'a', '1')
	True
	>>> c.ns_set('ns2', 'a', '2')
	True

Get one and flush the namespace:

	>>> c.ns_get('ns1', 'a')
	'1'
	>>> c.ns_flush('ns1')
	>>> c.ns_get('ns1', 'a')

Increment and decrement a variable:

	>>> c.ns_get('ns2', 'a')
	'2'
	>>> c.ns_incr('ns2', 'a')
	3
	>>> c.ns_get('ns2', 'a')
	'3'
	>>> c.ns_decr('ns2', 'a')
	2

Remove a variable from the namespace:

	>>> c.ns_delete('ns2', 'a')
	1
	>>> c.ns_get('ns2', 'a')

"""

__author__ = 'Savu Andrei <contact@andreisavu.ro>'

import memcache

class Client(memcache.Client):
    ' A modified version of the memcache client class with namespaces '

    def __init__(self, servers, debug = 0):
        """ Standard memcache class initialization """
        memcache.Client.__init__(self, servers, debug=debug)


    def _get_key(self, ns, key):
        """ 
		Get the real key for the virtual key in the given namespace

		This function should be used only internally. This is how the 
		fake namespaces work. For each pair (ns,key) 

			>>> c = Client(['localhost:11211'])
			>>> c.flush_all()
			>>> c._get_key('ns1', 'k1')
			'__ns1_1_k1'
			>>> c.get('__ns_ns1')
			'1'

		"""
        ns_key = ('__ns_%s' % ns)
        id = '1'
        if not(self.add(ns_key, id)):
            id = self.get(ns_key)
        return ('__%s_%s_%s' % (ns, id, key))

    def ns_flush(self, ns):
        """
		Flush all data from a given namespace. 

		The data will still remain in memory but the namespace key will change.
		Memcache will ensure the memory is reused as expected.
		"""
        ns_key = ('__ns_%s' % ns)
        if not(self.incr(ns_key)):
            self.set(ns_key, 1)



    def ns_add(self, ns, key, val, time = 0, min_compress = 0):
        """ Set a new variable if not already set """
        nk = self._get_key(ns, key)
        return self.add(nk, val, time, min_compress)



    def ns_set(self, ns, key, val, time = 0, min_compress = 0):
        """ Set the value for a key """
        nk = self._get_key(ns, key)
        return self.set(nk, val, time, min_compress)



    def ns_get(self, ns, key):
        """ Get the value stored for a key """
        nk = self._get_key(ns, key)
        return self.get(nk)



    def ns_incr(self, ns, key, amount = 1):
        """ Increment a variable by the given amount """
        nk = self._get_key(ns, key)
        return self.incr(nk, amount)



    def ns_decr(self, ns, key, amount = 1):
        """ Decrement a variable by the given amount """
        nk = self._get_key(ns, key)
        return self.decr(nk, amount)



    def ns_delete(self, ns, key):
        """ Delete a variable from the namespace """
        nk = self._get_key(ns, key)
        return self.delete(nk)



if (__name__ == '__main__'):
    import doctest
    doctest.testmod()

# local variables:
# tab-width: 4
