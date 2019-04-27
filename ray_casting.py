from collections import namedtuple

Pt = namedtuple('Pt', 'x, y')                 # Point
Edge = namedtuple('Edge', 'a, b')             # Polygon edge from a to b
Equation = namedtuple('Equation', 'a, b, c')  # Equation of a line

_epsilon = 0.0000001


def _get_line_equation(edge):
    a = edge.b.y - edge.a.y
    b = edge.a.x - edge.b.x
    c = (edge.b.x * edge.a.y) - (edge.a.x * edge.b.y)
    return Equation(a=a, b=b, c=c)


def _check_line_equation(equation, point):
    return equation.a * point.x + equation.b * point.y + equation.c


def _has_same_sign(n1, n2):
    return (n1 > 0 and n2 > 0) or (n1 < 0 and n2 < 0)


def _equals(n1, n2):
    return abs(n1 - n2) <= _epsilon


def _ray_intersects_edge(ray, edge):
    """
    Takes a point p = Pt() and an edge of two endpoints a, b = Pt() of a line segment and returns a boolean
    """

    # convert ray to a line of infinite length (so it satisfies a1 * x + b1 * y + c1 = 0 for all (x,y) on the line)
    ray_line = _get_line_equation(ray)
    ray_d1 = _check_line_equation(ray_line, edge.a)
    ray_d2 = _check_line_equation(ray_line, edge.b)

    # if d1 and d2 have the same sign, then both endpoints of edge are on the same side of the ray line
    if _has_same_sign(ray_d1, ray_d2):
        return False

    edge_line = _get_line_equation(edge)
    edge_d1 = _check_line_equation(edge_line, ray.a)
    edge_d2 = _check_line_equation(edge_line, ray.b)
    if _has_same_sign(edge_d1, edge_d2):
        return False

    # either vectors intersects at excactly one point or they are colinear
    return not _equals((ray_line.a * edge_line.b) - (ray_line.b * edge_line.a), 0)


def _point_intersects_polygon(point):
    def intersects_polygon(polygon):
        if len(polygon) < 2:
            return False
        xs = list(map(lambda edge: edge.a.x, polygon))
        min_x = min(xs)
        ray_a = Pt(x=min_x, y=point.y)
        ray_b = point
        ray = Edge(a=ray_a, b=ray_b)
        intersections = 0
        for edge in polygon:
            if _ray_intersects_edge(ray, edge):
                intersections += 1
        return intersections % 2 == 1
    return intersects_polygon


def _ring2edges(ring):
    """
    Transforms a ring into a list of edges.
    Assumes that the ring starts and ends at the same coordinate.
    """
    edges = []
    for i in range(len(ring) - 1):
        lon1, lat1 = ring[i]
        lon2, lat2 = ring[i + 1]
        a = Pt(x=lon1, y=lat1)
        b = Pt(x=lon2, y=lat2)
        edge = Edge(a=a, b=b)
        edges.append(edge)
    return edges


def point_intersects_multi_polygon(point, multi_polygon):
    lon, lat = point
    point = Pt(x=lon, y=lat)
    intersects = _point_intersects_polygon(point)
    for polygon in multi_polygon:
        exterior_ring = _ring2edges(polygon[0])
        interior_rings = list(map(_ring2edges, polygon[1:]))
        intersects_exterior = intersects(exterior_ring)
        intersects_interior = any(map(intersects, interior_rings))
        if intersects_exterior and not intersects_interior:
            return True
    return False
