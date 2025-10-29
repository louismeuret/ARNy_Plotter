import numpy as np
import mdtraj as md
from struct import pack, unpack, calcsize
from itertools import combinations
import matplotlib.pyplot as plt
def get_contact_map_and_signature(
    native_structure_file_name,
    atoms_selection='all',
    min_residues_apart=0,
    distance_scale=7.5):
    """
    This function computes the contact map and the signature of the given native structure file.
    
    Parameters
    ----------
    native_structure_file_name: str
        A pdb/gro file containing the native structure.
    atoms_selection: str
        A string specifying the atoms to keep in computation. Default is 'all'.
    min_residues_apart: int
        The minimum number of residues that must separate atom pairs in the output signature. Default is 4.
    distance_scale: float
        The characteristic distance scale (Angstroms). Default is 7.5.
    
    Returns
    -------
    contact_map: np.ndarray
        The contact map of the native structure.
    signature: np.ndarray
        The signature of atom-atom contacts in the native structure.
    """

    # Load the native structure from the input file
    native_structure = md.load(native_structure_file_name)
    
    # Get the indices of the selected atoms
    atom_indices = native_structure.topology.select(atoms_selection)

    # Get the indices of the corresponding residues
    residue_indices = [
        atom.residue.index for atom in native_structure.topology.atoms
        if atom.index in atom_indices]

    # Generate the atom pairs signature based on the minimum residue distance
    signature = []
    for i in range(len(atom_indices)):
        for j in range(i + 1, len(atom_indices)):
            if residue_indices[j] - residue_indices[i] >= min_residues_apart:
                signature.append([atom_indices[i], atom_indices[j]])
    
    # Compute the distances of the atom pairs and rescale them
    signature = np.array(signature)
    distances = md.compute_distances(native_structure, signature)[0]
    distances = distances / (distance_scale / 10.)  # Angstroms to nm conversion

    # Compute the contact map based on the computed distances
    contact_map = np.zeros(distances.shape)
    where_zero = distances == 0
    where_flexus = np.abs(distances - 1) < 1e-3
    contact_map[where_zero] = 1.0
    contact_map[where_flexus] = 0.6
    mask = ~ (where_zero | where_flexus)
    contact_map[mask] = (1 - distances[mask] ** 6) / (1 - distances[mask] ** 10)
    
    return contact_map, signature


def write_contact_map(
    contact_map_file_name, contact_map, signature,
    distance_scale=7.5):
    """
    This function writes the contact map and signature to a binary file,
    in a format that can by handled by GROMACS' ratchet bias.
    
    Parameters
    ----------
    contact_map_file_name: str
        The name of the output file.
    contact_map: np.ndarray
        The contact map to be written to the file.
    signature: np.ndarray
        The signature of the contact map.
    distance_scale: float
        The characteristic distance scale (Angstroms). Default is 7.5.
    """
    
    # Custom settings
    progress = 1.0
    normalization = 1.0 / np.sum(contact_map ** 2)
    
    # Increment the signature by one
    signature += 1
        
    # Get the unique atom indices and the inverted signature
    atom_indices = np.unique(np.array(signature, dtype=np.uint32).flatten())
    inverted_signature = np.asarray(signature, dtype=np.uint32)[:, ::-1]
    sorted_indices = np.lexsort((inverted_signature[:, 1], inverted_signature[:, 0]))
    inverted_signature = inverted_signature[sorted_indices]
    
    # Generate the full signature
    full_signature = np.array([(j, i) for i, j in combinations(atom_indices, 2)], dtype=np.uint32)
    full_signature = full_signature[np.lexsort((full_signature[:, 1], full_signature[:, 0]))]

    # Compute the mask
    cur_pos = 0
    c_0 = True
    mask = []
    n_zeros = 0
    n_ones = 0
    for i in range(len(full_signature)):
        if c_0:
            if not sum([x != y for x, y in zip(full_signature[i], inverted_signature[cur_pos])]):
                # The two arrays are the same
                mask.append(n_zeros)
                c_0 = False
                n_zeros = 0
                n_ones += 1
            else:
                if n_zeros == 65535:
                    mask.append(n_zeros)
                    mask.append(0)
                    n_zeros = 0
                n_zeros += 1
        else:
            if cur_pos < len(inverted_signature) - 1:
                cur_pos += 1
            if not sum([x != y for x, y in zip(full_signature[i], inverted_signature[cur_pos])]):
                # The two arrays are the same
                if n_ones == 65535:
                    mask.append(n_ones)
                    mask.append(0)
                    n_ones = 0
                n_ones += 1
            else:
                mask.append(n_ones)
                c_0 = True
                n_ones = 0
                n_zeros += 1
    if c_0:
        mask.append(n_zeros)
    else:
        mask.append(n_ones)

    # Write the contact map to the output file
    with open(contact_map_file_name, 'wb') as f:
        f.write(pack('4c', b'C', b'M', b'A', b'P'))
        f.write(pack('I', len(atom_indices)))
        f.write(pack('4c', *[x.encode('UTF-8') for x in 'ABCC']))
        f.write(pack('d', distance_scale))
        f.write(atom_indices.tobytes())
        f.write(pack('I', len(mask)))
        f.write(np.array(mask, dtype=np.uint16).tobytes())
        f.write(pack('I', 1))
        f.write(pack('I', 2))
        f.write(pack('d', progress))
        f.write(pack('d', normalization))
        f.write((contact_map[sorted_indices] * 65535 + 0.5).astype(np.uint16).tobytes())
        
    


import numpy as np
import mdtraj as md
from struct import pack, unpack, calcsize
from itertools import combinations
import plotly.graph_objs as go
import plotly.io as pio

def read_contact_map(contact_map_file_name, path_save_figure):
    """
    Retrieve information from a contact map input binary file.

    Parameters
    ----------
    contact_map_file_name : str, input filename

    Returns
    -------
    ctype   : str, e.g. "ABCC"
    unit    : float, characteristic length scale of the function used
              to compute the value assigned to each contact
    signature : list of tuples of two integers, signature of all the
                possible (non-repeating) pairs of contacts for the
                molecular system; indexes start counting from one in
                the input file, but are modified such that they start from zero
    colvar  : array-like of float, progress value (0. to 1.) assigned to
              each contact map written in the input file
    lambdas : array-like of float, weight assigned to each contact map
              written in the input file
    cmap    : array-like of array-like of floats; list of (linearized,
              according to signature) contact maps written in the input file
    """

    # Read input file
    with open(contact_map_file_name, 'rb') as f:
        f.read(calcsize('4c'))
        len_ext_ind = unpack('I', f.read(calcsize('I')))[0]
        c_type = ''.join([x.decode('UTF-8') for x in unpack('4c', f.read(calcsize('4c')))])
        unit = unpack('d', f.read(calcsize('d')))[0]
        ext_indexes = np.frombuffer(f.read(4 * len_ext_ind), dtype=np.uint32)

        # Retrieve signature as a boolean mask
        indexes_all = np.array([(j, i) for i, j in combinations(ext_indexes, 2)])
        indexes_all = indexes_all[np.lexsort((indexes_all[:, 1], indexes_all[:, 0]))]
        len_mask = unpack('I', f.read(calcsize('I')))[0]
        mask = np.frombuffer(f.read(2 * len_mask), dtype=np.uint16)
        bool_mask = np.full(indexes_all.shape[0], False)
        c_0 = True
        cur_pos = 0
        for i in mask:
            if c_0:
                cur_pos += i
            else:
                bool_mask[cur_pos:cur_pos + i] = True
                cur_pos += i
            c_0 = not c_0

        # Sort indexes
        indexes = indexes_all[bool_mask]
        lower2upper = np.lexsort((indexes[:, 0], indexes[:, 1]))
        indexes = indexes[lower2upper, ::-1]

        # Initialize output
        n_frames = unpack('I', f.read(calcsize('I')))[0]
        precision = unpack('I', f.read(calcsize('I')))[0]
        colvar = np.empty(n_frames)
        lambdas = np.empty(n_frames)
        cmaps = np.empty((n_frames, indexes.shape[0]))

        # Loop through frames
        for i in range(n_frames):
            colvar[i] = unpack('d', f.read(calcsize('d')))[0]
            lambdas[i] = unpack('d', f.read(calcsize('d')))[0]
            cmaps[i] = np.frombuffer(f.read(precision * indexes.shape[0]), dtype=np.uint16) / 65535
            cmaps[i] = cmaps[i, lower2upper]

        # Determine the size of the contact map
        max_index = max(indexes.flatten())
        cmap_mat = np.zeros((max_index + 1, max_index + 1))

        for x in range(len(indexes)):
            cmap_mat[indexes[x][0]][indexes[x][1]] = cmaps[0][x]

        # Mirror the lower triangle to the upper triangle to make it symmetric
        for i in range(cmap_mat.shape[0]):
            for j in range(i+1, cmap_mat.shape[1]):
                cmap_mat[j][i] = cmap_mat[i][j]

        # Create Plotly heatmap
        fig = go.Figure(data=go.Heatmap(
            z=cmap_mat,
            colorscale='Viridis'
        ))

        fig.update_layout(
            title='Contact Map',
            xaxis=dict(title='Residue Index'),
            yaxis=dict(title='Residue Index')
        )

        # Save plot to file
        pio.write_image(fig, path_save_figure)

    return (c_type, unit, indexes - 1, colvar, lambdas, cmaps)

